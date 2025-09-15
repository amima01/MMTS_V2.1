namespace MMTS.ML

open System
open System.Collections.Generic
open YamlDotNet.RepresentationModel
open Types

module Parse =

    let private toStr (n: YamlNode) = (n :?> YamlScalarNode).Value

    let private tryGet (m: YamlMappingNode) (key: string) =
        let k = YamlScalarNode(key) :> YamlNode
        if m.Children.ContainsKey k then Some m.Children.[k] else None

    let private asMap (n: YamlNode) = n :?> YamlMappingNode
    let private asSeq (n: YamlNode) = n :?> YamlSequenceNode

    let private ioOf (s:string) =
        match s.Trim().ToLowerInvariant() with
        | "rows" -> IoKind.Rows
        | "json" -> IoKind.Json
        | _      -> IoKind.Nonx

    let private stepKindOf (s:string option) =
        match s with
        | Some k when k.Trim().ToLowerInvariant() = "materialize" -> StepKind.Materialize
        | _ -> StepKind.Normal

    /// Flatten a YAML mapping node into "prefix.child" keys
    let rec private flattenMap (prefix:string) (m: YamlMappingNode) (dest: IDictionary<string,obj>) =
        for kv in m.Children do
            let key = (kv.Key :?> YamlScalarNode).Value
            let fullKey = if String.IsNullOrEmpty prefix then key else $"{prefix}.{key}"
            match kv.Value with
            | :? YamlScalarNode as sc ->
                dest.[fullKey] <- box sc.Value
            | :? YamlMappingNode as mm ->
                flattenMap fullKey mm dest
            | :? YamlSequenceNode as sq ->
                let arr = ResizeArray<string>()
                for item in sq.Children do
                    match item with
                    | :? YamlScalarNode as isc -> arr.Add(isc.Value)
                    | _ -> arr.Add(item.ToString())
                dest.[fullKey] <- box arr
            | other ->
                dest.[fullKey] <- box (other.ToString())

    /// Very small, permissive parser for the subset we use in S0
    let tryParse (yamlText: string) : Result<WorkflowSpec, string> =
        try
            let ys = YamlStream()
            ys.Load(new System.IO.StringReader(yamlText))
            let root = ys.Documents.[0].RootNode |> asMap

            let name    = tryGet root "name"    |> Option.map toStr |> Option.defaultValue "(unnamed)"
            let version = tryGet root "version" |> Option.map toStr |> Option.defaultValue "1.0"

            // params
            let prms : IDictionary<string,string> = Dictionary<string,string>(StringComparer.OrdinalIgnoreCase) :> _
            match tryGet root "params" with
            | Some p ->
                let pm = asMap p
                for kv in pm.Children do
                    let k = (kv.Key :?> YamlScalarNode).Value
                    let v =
                        match kv.Value with
                        | :? YamlScalarNode as sc -> sc.Value
                        | _ -> kv.Value.ToString()
                    (prms :?> Dictionary<string,string>).[k] <- v
            | None -> ()

            // hooks
            let hooks : IDictionary<string,HookSpec> = Dictionary<string,HookSpec>(StringComparer.OrdinalIgnoreCase) :> _
            match tryGet root "hooks" with
            | Some h ->
                let hm = asMap h
                for kv in hm.Children do
                    let name = (kv.Key :?> YamlScalarNode).Value
                    let v    = asMap kv.Value
                    let modl = tryGet v "module" |> Option.map toStr |> Option.defaultValue ""
                    let func = tryGet v "func"   |> Option.map toStr |> Option.defaultValue "Run"
                    let kind = tryGet v "kind"   |> Option.map toStr |> Option.defaultValue "none"
                    (hooks :?> Dictionary<string,HookSpec>).[name] <- { Module = modl; Func = func; Kind = kind }
            | None -> ()

            // steps
            let steps =
                match tryGet root "steps" with
                | Some s ->
                    [ for n in asSeq s do
                        let m = asMap n
                        let id = tryGet m "id" |> Option.map toStr |> Option.defaultValue ""
                        let kind = tryGet m "kind" |> Option.map toStr |> stepKindOf

                        let parseIn() =
                            let iM =
                                match tryGet m "in" with
                                | Some x -> asMap x
                                | None -> YamlMappingNode()
                            let from = tryGet iM "from" |> Option.map toStr
                            let io =
                                match tryGet iM "io" with
                                | Some x -> ioOf (toStr x)
                                | None -> IoKind.Nonx
                            { from = from; io = io }

                        let parseOut() =
                            let oM =
                                match tryGet m "out" with
                                | Some x -> asMap x
                                | None -> YamlMappingNode()
                            let io =
                                match tryGet oM "io" with
                                | Some x -> ioOf (toStr x)
                                | None -> IoKind.Nonx
                            { io = io }

                        let parseArgs() =
                            let aM =
                                match tryGet m "args" with
                                | Some x -> asMap x
                                | None -> YamlMappingNode()
                            let d = Dictionary<string,obj>(StringComparer.OrdinalIgnoreCase) :> IDictionary<string,obj>
                            // direct scalars + flatten nested maps + capture sequences
                            for kv in aM.Children do
                                match kv.Value with
                                | :? YamlScalarNode as sc ->
                                    d.[(kv.Key :?> YamlScalarNode).Value] <- box sc.Value
                                | :? YamlMappingNode as mm ->
                                    flattenMap ((kv.Key :?> YamlScalarNode).Value) mm d
                                | :? YamlSequenceNode as sq ->
                                    let arr = ResizeArray<string>()
                                    for item in sq.Children do
                                        match item with
                                        | :? YamlScalarNode as isc -> arr.Add(isc.Value)
                                        | _ -> arr.Add(item.ToString())
                                    d.[(kv.Key :?> YamlScalarNode).Value] <- box arr
                                | other ->
                                    d.[(kv.Key :?> YamlScalarNode).Value] <- box (other.ToString())
                            d

                        { Id = id
                          Kind = kind
                          Inp  = parseIn()
                          Out  = parseOut()
                          Args = parseArgs() } ]
                | None -> []

            Ok { Name = name; Version = version; Params = prms; Hooks = hooks; Steps = steps }
        with ex ->
            Error ex.Message
