namespace MMTS.ML
(*here’s the single, fully-updated MMTS.ML.Loader.fs with cron schedule (Quartz),
parallel groups with degree-of-parallelism,
and richer foreach sources. it’s all 4-space indented—drop this in and build.*)

open System
open System.Threading
open System.Collections.Generic
open System.Text.RegularExpressions
open YamlDotNet.RepresentationModel


// -----------------------------
// YAML helpers
// -----------------------------
module private Y =
    let str (n:YamlNode) =
        match n with
        | :? YamlScalarNode as s -> s.Value |> string
        | _ -> failwithf "Expected scalar, got %A" n

    let asMap (n:YamlNode) =
        match n with
        | :? YamlMappingNode as m -> m
        | _ -> failwithf "Expected map, got %A" n

    let asSeq (n:YamlNode) =
        match n with
        | :? YamlSequenceNode as s -> s
        | _ -> failwithf "Expected seq, got %A" n

    let kvs (m:YamlMappingNode) =
        m.Children
        |> Seq.choose (fun kv ->
            match kv.Key with
            | :? YamlScalarNode as k -> Some (k.Value, kv.Value)
            | _ -> None)
        |> dict

// -----------------------------
// Interpolation
// -----------------------------
module Interp =
    open Types

    // ${params.x} interpolation and env:VAR resolution
    let private reParam = Regex(@"\$\{params\.([A-Za-z0-9_]+)\}", RegexOptions.Compiled)

    let private isEnvRef (s:string) =
        s.StartsWith("env:", StringComparison.OrdinalIgnoreCase)

    let apply (p:Params) (s:string) =
        if isNull s then s else
        let replaced =
            reParam.Replace(
                s,
                fun (m:Match) ->
                    match m.Groups[1].Value with
                    | "envName"  -> p.envName
                    | "dbName"   -> p.dbName
                    | "connStr"  -> p.connStr
                    | "hookNs"   -> p.hookNs
                    | "impl"     -> p.impl
                    | "batchId"  -> p.batchId
                    | "emitMode" -> p.emitMode
                    | k -> failwithf "Unknown params.%s" k
            )
        if isEnvRef replaced then
            let key = replaced.Substring("env:".Length)
            let v = Environment.GetEnvironmentVariable(key)
            if String.IsNullOrWhiteSpace v then
                failwithf "Environment variable '%s' not set." key
            v
        else
            replaced

    let applyOpt p = Option.map (apply p)

// -----------------------------
// YAML -> AST
// -----------------------------
module Parse2 =
    open Types
    open Y
    open Interp

    let private readParams (mp:IDictionary<string,YamlNode>) =
        let s k d =
            mp.TryGetValue k
            |> function
                | true, v -> Y.str v
                | _ -> d
        { envName  = s "envName" "dev"
          dbName   = s "dbName"  "0_CADENCE"
          connStr  = s "connStr" ""
          hookNs   = s "hookNs"  "POC.Hooks"
          impl     = s "impl"    "v1"
          batchId  = s "batchId" "YYYY-MM-DD"
          emitMode = s "emitMode" "bulk_upsert" }

    let private readDatasources (nd:YamlNode) =
        let m = asMap nd
        m.Children
        |> Seq.map (fun kv ->
            let name = (kv.Key :?> YamlScalarNode).Value
            let mm = asMap kv.Value |> kvs
            let kind = str mm["kind"]
            let conn = str mm["conn"]
            name, { kind = kind; conn = conn })
        |> Map.ofSeq

    let private readTables (nd:YamlNode) =
        let m = asMap nd
        m.Children
        |> Seq.map (fun kv ->
            let name = (kv.Key :?> YamlScalarNode).Value
            let mm = asMap kv.Value |> kvs
            let ds   = str mm["ds"]
            let tnam = str mm["name"]
            name, { ds = ds; name = tnam })
        |> Map.ofSeq

    let private readHooks (nd:YamlNode) =
        let m = asMap nd
        m.Children
        |> Seq.map (fun kv ->
            let key = (kv.Key :?> YamlScalarNode).Value
            let mm = asMap kv.Value |> kvs
            key,
                { moduleName = str mm["module"]
                  func       = str mm["func"]
                  kind       = str mm["kind"] })
        |> Map.ofSeq

    let private readStepIO (mm:IDictionary<string,YamlNode>) (key:string) : Types.StepIO option =
        match mm.TryGetValue key with
        | true, v ->
            let m = asMap v |> kvs
            let rows =
                m.TryGetValue "rows"
                |> function
                    | true, x -> Some (str x)
                    | _ -> None
            Some { rows = rows }
        | _ -> None

    let private readArgs (mm:IDictionary<string,YamlNode>) =
        match mm.TryGetValue "args" with
        | true, v ->
            let m = asMap v
            m.Children
            |> Seq.choose (fun kv ->
                match kv.Key, kv.Value with
                | :? YamlScalarNode as k, (:? YamlScalarNode as s) -> Some (k.Value, s.Value)
                | _ -> None)
            |> Map.ofSeq
        | _ -> Map.empty

    // ---- uses parsing (scalar or object) ----
    let private parseIoKind (s:string) =
        match s.Trim().ToLowerInvariant() with
        | "rows" -> IoKind.Rows
        | "json" -> IoKind.Json
        | _      -> IoKind.Nonx

    let private parseTime (s:string) =
        System.TimeSpan.Parse s  // supports "hh:mm:ss"

    let private readUse (n:YamlNode) : Use =
        match n with
        | :? YamlScalarNode as sc ->
            // scalar string → HookKey (back-compat)
            HookKey sc.Value
        | :? YamlMappingNode as m ->
            let mm = kvs m
            let ty =
                match mm.TryGetValue "type" with
                | true, v -> (str v).Trim().ToLowerInvariant()
                | _ ->
                    if mm.ContainsKey "typeName" || mm.ContainsKey "module" then "dotnet" else "dotnet"
            match ty with
            | "exe" ->
                let path    = str mm["path"]
                let args =
                    match mm.TryGetValue "args" with
                    | true, (:? YamlSequenceNode as s) -> [ for a in s.Children -> str a ]
                    | true, (:? YamlScalarNode as s) -> [ s.Value ]
                    | _ -> []
                let stdin  =
                    match mm.TryGetValue "stdin" with
                    | true, v -> parseIoKind (str v)
                    | _ -> IoKind.Nonx
                let stdout =
                    match mm.TryGetValue "stdout" with
                    | true, v -> parseIoKind (str v)
                    | _ -> IoKind.Rows
                let timeout =
                    match mm.TryGetValue "timeout" with
                    | true, v -> parseTime (str v)
                    | _ -> System.TimeSpan.FromMinutes 5.0
                let retry =
                    match mm.TryGetValue "retry" with
                    | true, (:? YamlMappingNode as rm) ->
                        let r = kvs rm
                        let max   = r.TryGetValue "max"   |> function true, v -> int (str v) | _ -> 0
                        let delay = r.TryGetValue "delay" |> function true, v -> parseTime (str v) | _ -> System.TimeSpan.FromSeconds 5.0
                        let b     = r.TryGetValue "backoff"|> function true, v -> float (str v) | _ -> 1.0
                        Some { max = max; delay = delay; backoff = b }
                    | _ -> None
                Exe { path = path; args = args; stdin = stdin; stdout = stdout; timeout = timeout; retry = retry }
            | _ ->
                // dotnet (module/typeName + method/func)
                let typeName =
                    match mm.TryGetValue "typeName" with
                    | true, v -> str v
                    | _ ->
                        match mm.TryGetValue "module" with
                        | true, v -> str v
                        | _ -> failwith "uses.dotnet requires 'typeName' (or 'module')"
                let methodName =
                    match mm.TryGetValue "method" with
                    | true, v -> str v
                    | _ ->
                        match mm.TryGetValue "func" with
                        | true, v -> str v
                        | _ -> "Run"
                DotNet { typeName = typeName; method = methodName }
        | _ ->
            failwithf "Unsupported 'uses' node: %A" n

    // ---- foreach spec ----
    let private readForeach (mm:IDictionary<string,YamlNode>) : ForeachSpec option =
        match mm.TryGetValue "foreach" with
        | false, _ -> None
        | true, v ->
            match v with
            | :? YamlScalarNode as s ->
                Some { over = Some s.Value; items = None; parallelism = None }
            | :? YamlMappingNode as m ->
                let kv = kvs m
                let overOpt =
                    match kv.TryGetValue "over" with
                    | true, n -> Some (str n)
                    | _ -> None
                let itemsOpt =
                    match kv.TryGetValue "items" with
                    | true, (:? YamlSequenceNode as seq) ->
                        [ for x in seq.Children -> str x ] |> Some
                    | _ -> None
                let dop =
                    match kv.TryGetValue "parallelism" with
                    | true, n -> Some (int (str n))
                    | _ ->
                        match kv.TryGetValue "dop" with
                        | true, n -> Some (int (str n))
                        | _ -> None
                if overOpt.IsNone && itemsOpt.IsNone then failwith "foreach requires 'over' or 'items'"
                Some { over = overOpt; items = itemsOpt; parallelism = dop }
            | _ -> failwith "Invalid 'foreach' node"

    // ---- parse a single Step from a mapping node ----
    let private readStepFromMap (mm:IDictionary<string,YamlNode>) : Step =
        { id       = str mm["id"]
          uses     = readUse mm["uses"]
          ``in``   = readStepIO mm "in"
          args     = readArgs mm
          foreach  = readForeach mm
          out      = readStepIO mm "out" }

    // ---- StepNode parsing (plain step OR parallel group with dop) ----
    let private readStepNode (n:YamlNode) : StepNode =
        match n with
        | :? YamlMappingNode as m ->
            let kv = kvs m
            if kv.ContainsKey "parallel" then
                let (kids, dop) =
                    match kv.["parallel"] with
                    | :? YamlSequenceNode as seq ->
                        // style A:
                        // - parallel:
                        //     - id: a ...
                        //     - id: b ...
                        //   parallelism: 3
                        let steps = [ for child in seq.Children -> readStepFromMap (asMap child |> kvs) ]
                        let dop =
                            match kv.TryGetValue "parallelism" with
                            | true, n -> Some (int (str n))
                            | _ ->
                                match kv.TryGetValue "dop" with
                                | true, n -> Some (int (str n))
                                | _ -> None
                        steps, dop
                    | :? YamlMappingNode as pmap ->
                        // style B:
                        // - parallel:
                        //     steps:
                        //       - id: a ...
                        //       - id: b ...
                        //     parallelism: 3
                        let pkv = kvs pmap
                        let steps =
                            match pkv.TryGetValue "steps" with
                            | true, (:? YamlSequenceNode as seq) ->
                                [ for child in seq.Children -> readStepFromMap (asMap child |> kvs) ]
                            | _ -> failwith "parallel.steps is required"
                        let dop =
                            match pkv.TryGetValue "parallelism" with
                            | true, n -> Some (int (str n))
                            | _ ->
                                match pkv.TryGetValue "dop" with
                                | true, n -> Some (int (str n))
                                | _ -> None
                        steps, dop
                    | _ -> failwith "parallel must be a sequence or a mapping"
                Parallel { steps = kids; parallelism = dop }
            else
                Single (readStepFromMap kv)
        | _ ->
            failwith "Each item in steps must be a mapping"

    let private readSteps (nd:YamlNode) : StepNode list =
        let s = asSeq nd
        [ for n in s.Children -> readStepNode n ]

    let private readValidations (nd:YamlNode) =
        let s = asSeq nd
        [ for n in s.Children ->
            let mm = asMap n |> kvs
            { id   = str mm["id"]
              on   = str mm["on"]
              rule = str mm["rule"] } ]

    let private readMaterialize (nd:YamlNode) =
        let s = asSeq nd
        [ for n in s.Children ->
            let mm = asMap n |> kvs
            let key =
                match mm.TryGetValue "key" with
                | true, v ->
                    match v with
                    | :? YamlSequenceNode as arr ->
                        [ for x in arr.Children -> str x ]
                    | :? YamlScalarNode as sc ->
                        [ sc.Value ]
                    | _ -> []
                | _ -> []
            { from    = str mm["from"]
              ``to``  = str mm["to"]
              mode    = str mm["mode"]
              key     = key
              preSql  = mm.TryGetValue "pre_sql"  |> function true, v -> Some (str v) | _ -> None
              postSql = mm.TryGetValue "post_sql" |> function true, v -> Some (str v) | _ -> None } ]

    // ---- schedule ----
    let private readSchedule (root:IDictionary<string,YamlNode>) : ScheduleSpec option =
        match root.TryGetValue "schedule" with
        | false, _ -> None
        | true, nd ->
            let m = asMap nd |> kvs
            let cron = m.TryGetValue "cron" |> function true, v -> str v | _ -> failwith "schedule.cron is required"
            Some { cron = cron }

    let load (yamlText:string) : Types.Spec =
        let ys = YamlStream()
        ys.Load(new System.IO.StringReader(yamlText))
        let root = ys.Documents.[0].RootNode |> asMap |> kvs

        let workflow    = str root["workflow"]
        let version     = str root["version"]
        let description = str root["description"]

        let paramsMap   = asMap root["params"] |> kvs
        let p0          = readParams paramsMap

        let datasources = readDatasources root["datasources"]
        let tables      = readTables      root["tables"]
        let hooks       = readHooks       root["hooks"]
        let steps       = readSteps       root["steps"]
        let validations = readValidations root["validations"]
        let materialize = readMaterialize root["materialize"]
        let schedule    = readSchedule   root

        { workflow    = workflow
          version     = version
          description = description
          parameters  = p0
          datasources = datasources
          tables      = tables
          hooks       = hooks
          steps       = steps
          validations = validations
          materialize = materialize
          schedule    = schedule }

    /// Apply interpolation across all relevant strings (including Use + hooks.func)
    let interpolateAll (sp:Types.Spec) : Types.Spec =
        let p = sp.parameters
        let mapDS =
            sp.datasources
            |> Map.map (fun _ v -> { v with conn = Interp.apply p v.conn })
        let mapTables =
            sp.tables
            |> Map.map (fun _ v -> { v with name = Interp.apply p v.name })
        let mapHooks =
            sp.hooks
            |> Map.map (fun _ v ->
                { v with
                    moduleName = Interp.apply p v.moduleName
                    func       = Interp.apply p v.func })
        let mapStep (s:Step) =
            let args' = s.args |> Map.map (fun _ v -> Interp.apply p v)
            let uses' =
                match s.uses with
                | HookKey k -> HookKey (Interp.apply p k)
                | DotNet d  -> DotNet { d with
                                            typeName = Interp.apply p d.typeName
                                            method   = Interp.apply p d.method }
                | Exe e     -> Exe { e with
                                            path = Interp.apply p e.path
                                            args = e.args |> List.map (Interp.apply p) }
            let foreach' =
                s.foreach
                |> Option.map (fun f ->
                    { f with
                        over  = f.over  |> Option.map (Interp.apply p)
                        items = f.items |> Option.map (List.map (Interp.apply p)) })
            { s with uses = uses'; args = args'; foreach = foreach' }
        let mapSteps =
            sp.steps
            |> List.map (function
                | Single s      -> Single (mapStep s)
                | Parallel g    -> Parallel { g with steps = g.steps |> List.map mapStep })
        let mapMats =
            sp.materialize
            |> List.map (fun m ->
                { m with from    = Interp.apply p m.from
                         ``to``  = Interp.apply p m.``to``
                         mode    = Interp.apply p m.mode
                         preSql  = Interp.applyOpt p m.preSql
                         postSql = Interp.applyOpt p m.postSql })
        { sp with datasources = mapDS
                  tables      = mapTables
                  hooks       = mapHooks
                  steps       = mapSteps
                  materialize = mapMats }

// -----------------------------
// Validations
// -----------------------------
module Validate =
    open Types

    type ValError = { id:string; message:string }

    let private need (rows:Rows) (id:string) (msg:string) ok =
        if ok then None else Some { id = id; message = msg }

    let nonempty id (rows:Rows) =
        need rows id "result is empty" (not rows.IsEmpty)

    let schemaMatch id (rows:Rows) (cols:string list) =
        let ok =
            rows.IsEmpty ||
            (let keys = rows.Head |> Map.toSeq |> Seq.map fst |> Set.ofSeq
             cols |> List.forall (fun c -> Set.contains c keys))
        need rows id (sprintf "schema mismatch; need [%s]" (String.Join(",", cols))) ok

    let range id (rows:Rows) (field:string) (mn:float) (mx:float) =
        let ok =
            rows
            |> List.forall (fun r ->
                match r |> Map.tryFind field with
                | Some (:? IConvertible as v) ->
                    let x = v.ToDouble Globalization.CultureInfo.InvariantCulture
                    x >= mn && x <= mx
                | Some _ -> false
                | None -> false)
        need rows id (sprintf "range(%s,%g,%g) failed" field mn mx) ok

    let unique id (rows:Rows) (field:string) =
        let seen = HashSet<string>()
        let ok =
            rows
            |> List.forall (fun r ->
                match r |> Map.tryFind field with
                | Some v ->
                    let k = string v
                    if seen.Contains k then false
                    else seen.Add k |> ignore; true
                | None -> false)
        need rows id (sprintf "unique(%s) failed" field) ok

    /// Dispatch a rule string like:
    ///   "nonempty"
    ///   "schemaMatch(Id, Name, Amount, BatchId)"
    ///   "range(Amount, 0, 100000000)"
    ///   "unique(Id)"
    let evalRule (id:string) (rule:string) (rows:Rows) : ValError option =
        let ruleName, args =
            let i = rule.IndexOf '('
            if i < 0 then rule.Trim(), [||]
            else
                let name = rule.Substring(0, i).Trim()
                let inside = rule.Substring(i + 1, rule.Length - i - 2)
                let parts =
                    inside.Split([|','|], StringSplitOptions.RemoveEmptyEntries)
                    |> Array.map (fun s -> s.Trim())
                name, parts
        match ruleName with
        | "nonempty"    -> nonempty id rows
        | "schemaMatch" -> schemaMatch id rows (args |> Array.toList)
        | "range" ->
            if args.Length <> 3 then Some { id = id; message = "range needs 3 args" }
            else
                let fld = args.[0]
                let mn  = Double.Parse args.[1]
                let mx  = Double.Parse args.[2]
                range id rows fld mn mx
        | "unique" ->
            if args.Length <> 1 then Some { id = id; message = "unique needs 1 arg" }
            else unique id rows args.[0]
        | x -> Some { id = id; message = sprintf "Unknown rule '%s'" x }

// -----------------------------
// Execution (hooks + materialize + exe bolts + parallel/foreach)
// -----------------------------
module Exec2 =
    open System
    open System.Diagnostics
    open System.Text
    open System.Text.Json
    open System.Threading
    open System.Threading.Tasks
    open System.Collections.Concurrent
    open Types
    open Validate

    // Simple resolver for step outputs in "scope:value" notation
    type Scope =
        | StepRows of string          // "step:<id>.rows"
        | TableRef of string          // "table:<name>"
        | Literal  of string          // e.g., "[db].dbo.Table"
        | Unknown  of string

    let parseRef (s:string) =
        if s.StartsWith("step:", StringComparison.OrdinalIgnoreCase) then StepRows s
        elif s.StartsWith("table:", StringComparison.OrdinalIgnoreCase) then TableRef (s.Substring(6))
        elif s.Contains("dbo.") || s.StartsWith("[") then Literal s
        else Unknown s

    // Hook invocation (dotnet)
    let private resolveType (fullName:string) =
        let t = Type.GetType(fullName, throwOnError = false)
        if not (isNull t) then t
        else
            AppDomain.CurrentDomain.GetAssemblies()
            |> Seq.tryPick (fun asm ->
                let tt = asm.GetType(fullName, throwOnError = false)
                if isNull tt then None else Some tt)
            |> Option.defaultWith (fun () ->
                raise (TypeLoadException (sprintf "Type '%s' not found in loaded assemblies." fullName)))

    let private callDotNet (typeName:string) (methodName:string) (inputs: obj array) : Rows =
        let t  = resolveType typeName
        let mi = t.GetMethod(methodName, Reflection.BindingFlags.Public ||| Reflection.BindingFlags.Static)
        if isNull mi then failwithf "Method not found: %s.%s" typeName methodName
        match mi.Invoke(null, inputs) with
        | :? Rows as r -> r
        | null -> []
        | other -> failwithf "Hook returned unexpected type: %A" (other.GetType())

    // --- tiny process runner (exe)
    type ExecOutcome = { exitCode:int; stdout:string; stderr:string }

    let private runProcess (path:string) (args:string list) (stdinText:string option) (timeout:TimeSpan) : ExecOutcome =
        let psi = ProcessStartInfo()
        psi.FileName <- path
        psi.Arguments <- String.Join(" ", args |> List.map (fun a -> if a.Contains " " then $"\"{a}\"" else a))
        psi.RedirectStandardInput  <- true
        psi.RedirectStandardOutput <- true
        psi.RedirectStandardError  <- true
        psi.UseShellExecute <- false
        psi.CreateNoWindow <- true

        use p = new Process()
        p.StartInfo <- psi
        if not (p.Start()) then failwithf "Failed to start process: %s" path

        match stdinText with
        | Some text ->
            p.StandardInput.Write(text)
            p.StandardInput.Flush()
            p.StandardInput.Close()
        | None ->
            p.StandardInput.Close()

        let outTask = p.StandardOutput.ReadToEndAsync()
        let errTask = p.StandardError.ReadToEndAsync()

        if not (p.WaitForExit(int timeout.TotalMilliseconds)) then
            try p.Kill(true) with _ -> ()
            failwithf "Process timed out: %s" path

        { exitCode = p.ExitCode; stdout = outTask.Result; stderr = errTask.Result }

    // rows <-> json helpers
    let private rowsToJson (rows: Rows) =
        let opts = JsonSerializerOptions(PropertyNamingPolicy = JsonNamingPolicy.CamelCase)
        JsonSerializer.Serialize(rows, opts)

    let private jsonToRows (text:string) : Rows =
        if String.IsNullOrWhiteSpace text then []
        else
            let opts = JsonSerializerOptions(PropertyNameCaseInsensitive = true)
            JsonSerializer.Deserialize<Rows>(text, opts)

    // callbacks you supply from runner for DB side effects
    type SqlExec = string -> unit

    type BulkEmitArgs =
        { conn  : string
          table : string
          mode  : string
          key   : string list
          rows  : Rows }

    type BulkEmit = BulkEmitArgs -> unit

    type ExecEnv =
        { spec     : Types.Spec
          stepRows : ConcurrentDictionary<string, Rows>  // thread-safe
          sqlExec  : SqlExec
          bulkEmit : BulkEmit }

    let private getRowsRef (env:ExecEnv) (refStr:string) : Rows =
        match parseRef refStr with
        | StepRows s ->
            // format: step:<id>.rows
            let id =
                let i = s.IndexOf ':'
                let j = s.LastIndexOf(".rows", StringComparison.OrdinalIgnoreCase)
                if i >= 0 && j > i then s.Substring(i + 1, j - i - 1) else s
            match env.stepRows.TryGetValue id with
            | true, r -> r
            | _ -> failwithf "No rows found for step '%s'" id
        | TableRef _ ->
            failwith "Reading from table is not implemented in this minimal runner."
        | Literal _ ->
            failwith "Literal inputs not supported for rows."
        | Unknown s ->
            failwithf "Unknown reference: %s" s

    // retry/backoff wrapper
    /// Type reminder:
    /// type RetrySpec = { max:int; delay:TimeSpan; backoff:float }
    /// type Rows = (your row type) list
    
    /// Synchronous retry with exponential backoff
    
    let private withRetry (spec: RetrySpec option) (f: unit -> Rows) : Rows =
        match spec with
        | None -> f()
        | Some r ->
            let rec go attempt (curDelay: TimeSpan) =
                try
                    f()
                with ex ->
                    if attempt >= r.max then
                        raise ex
                    else
                        // Sleep using TimeSpan overload (type-safe, no casts)
                        Thread.Sleep curDelay
                        // Exponential backoff (stay in TimeSpan space)
                        let nextDelay =
                            TimeSpan.FromMilliseconds(curDelay.TotalMilliseconds * r.backoff)
                        go (attempt + 1) nextDelay
            go 0 r.delay
    
    // ---- foreach helpers ----
    let private splitCsv (s:string) =
        s.Split([|','|], StringSplitOptions.RemoveEmptyEntries)
        |> Array.map (fun x -> x.Trim())
        |> Array.toList

    let private enumerateForeachItems (env:ExecEnv) (st:Step) : obj list =
        // Priority: items list > over expression
        match st.foreach with
        | None -> []
        | Some f when f.items.IsSome ->
            f.items.Value |> List.map box
        | Some f when f.over.IsSome ->
            let o = f.over.Value.Trim()
            if o.StartsWith("rows:", StringComparison.OrdinalIgnoreCase) then
                let rref = o.Substring("rows:".Length)
                let rows = getRowsRef env rref
                rows |> List.map (fun r -> box r)
            elif o.StartsWith("args:", StringComparison.OrdinalIgnoreCase) then
                let key = o.Substring("args:".Length)
                let raw =
                    st.args
                    |> Map.tryFind key
                    |> Option.defaultWith (fun () -> failwithf "foreach args key '%s' not found" key)
                splitCsv raw |> List.map box
            elif o.StartsWith("range:", StringComparison.OrdinalIgnoreCase) then
                let range = o.Substring("range:".Length)
                let parts = range.Split([|".."|], StringSplitOptions.None)
                if parts.Length <> 2 then failwith "range must be 'start..stop'"
                let a = int (parts.[0].Trim())
                let b = int (parts.[1].Trim())
                [a..b] |> List.map box
            elif o.StartsWith("file:", StringComparison.OrdinalIgnoreCase) then
                let path = o.Substring("file:".Length).Trim()
                if not (System.IO.File.Exists path) then failwithf "foreach file not found: %s" path
                System.IO.File.ReadAllLines path
                |> Array.toList
                |> List.map (fun s -> s.Trim())
                |> List.filter (fun s -> s <> "" && not (s.StartsWith("#")))
                |> List.map box
            elif o.StartsWith("env:", StringComparison.OrdinalIgnoreCase) then
                let var = o.Substring("env:".Length).Trim()
                let v = System.Environment.GetEnvironmentVariable var
                if String.IsNullOrWhiteSpace v then [] else splitCsv v |> List.map box
            elif o.StartsWith("json:", StringComparison.OrdinalIgnoreCase) then
                let json = o.Substring("json:".Length).Trim()
                if String.IsNullOrWhiteSpace json then []
                else
                    let doc = System.Text.Json.JsonDocument.Parse(json)
                    if doc.RootElement.ValueKind <> System.Text.Json.JsonValueKind.Array then
                        failwith "foreach json: expects a JSON array"
                    [ for e in doc.RootElement.EnumerateArray() ->
                        match e.ValueKind with
                        | System.Text.Json.JsonValueKind.String -> box (e.GetString())
                        | System.Text.Json.JsonValueKind.Number ->
                            let iOk, iVal = e.TryGetInt64()
                            if iOk then box iVal else box (e.GetDouble())
                        | _ -> box (e.ToString()) ]
            else
                failwithf "Unsupported foreach.over: %s (use rows:, args:, range:, file:, env:, json:)" o
        | _ -> []

    let private argsForItem (baseArgs:Map<string,string>) (item:obj) : Map<string,string> =
        // inject special keys so bolts can read the item
        let add (k,v) m = Map.add k v m
        match item with
        | :? int64 as i ->
            baseArgs |> add ("item", string i)
        | :? int as i ->
            baseArgs |> add ("item", string i)
        | :? double as d ->
            baseArgs |> add ("item", d.ToString(System.Globalization.CultureInfo.InvariantCulture))
        | :? string as s ->
            baseArgs |> add ("item", s)
        | :? Types.Row as row ->
            row
            |> Map.fold (fun acc k v -> Map.add ($"item.{k}") (string v) acc) baseArgs
            |> add ("item", "row")
        | other ->
            baseArgs |> add ("item", string other)

    // run a single step (with optional foreach fan-out)
    let private runOneStep (env:ExecEnv) (st:Step) : Rows =
        let executeOnce (rowsInOpt: Rows option) (args: Map<string,string>) : Rows =
            match st.uses with
            | HookKey key ->
                let hook =
                    env.spec.hooks
                    |> Map.tryFind key
                    |> Option.defaultWith (fun () -> failwithf "Hook '%s' not found in hooks map." key)
                let inputs =
                    match rowsInOpt with
                    | Some rows -> [| box rows; box args |]
                    | None -> [| box args |]
                callDotNet hook.moduleName hook.func inputs
            | DotNet dn ->
                let inputs =
                    match rowsInOpt with
                    | Some rows -> [| box rows; box args |]
                    | None -> [| box args |]
                callDotNet dn.typeName dn.method inputs
            | Exe ex ->
                let stdinText =
                    match ex.stdin, rowsInOpt with
                    | IoKind.Rows, Some rows -> rowsToJson rows |> Some
                    | IoKind.Rows, None      -> Some "[]"
                    | IoKind.Json, _         ->
                        let dict = args |> Map.toSeq |> dict
                        let opts = System.Text.Json.JsonSerializerOptions(PropertyNamingPolicy = System.Text.Json.JsonNamingPolicy.CamelCase)
                        Some (System.Text.Json.JsonSerializer.Serialize(dict, opts))
                    | IoKind.Nonx, _         -> None
                let doRun () =
                    let outcome = runProcess ex.path ex.args stdinText ex.timeout
                    if outcome.exitCode <> 0 then
                        let msg = if String.IsNullOrWhiteSpace outcome.stderr then outcome.stdout else outcome.stderr
                        failwithf "Process failed (%s): %s" ex.path msg
                    match ex.stdout with
                    | IoKind.Rows -> jsonToRows outcome.stdout
                    | _           -> []
                withRetry ex.retry doRun

        match st.foreach with
        | None ->
            // no fan-out
            let rowsInOpt =
                match st.``in`` with
                | Some i when i.rows.IsSome -> Some (getRowsRef env i.rows.Value)
                | _ -> None
            executeOnce rowsInOpt st.args
        | Some f ->
            // fan-out across items, respecting parallelism
            let items = enumerateForeachItems env st
            let dop =
                match f.parallelism with
                | Some x when x > 1 -> x
                | _ -> 1
            if dop <= 1 then
                // sequential
                let mutable acc : Rows = []
                for it in items do
                    let args' = argsForItem st.args it
                    let rowsInOpt =
                        match st.``in`` with
                        | Some i when i.rows.IsSome ->
                            // if item is a Row and input expects rows, pass [item]; else pass original rows
                            match it with
                            | :? Types.Row as r -> Some [ r ]
                            | _ -> Some (getRowsRef env i.rows.Value)
                        | _ -> None
                    let outRows = executeOnce rowsInOpt args'
                    acc <- acc @ outRows
                acc
            else
                // bounded parallelism
                let sem = new SemaphoreSlim(dop)
                let tasks =
                    [ for it in items ->
                        task {
                            do! sem.WaitAsync()
                            try
                                let args' = argsForItem st.args it
                                let rowsInOpt =
                                    match st.``in`` with
                                    | Some i when i.rows.IsSome ->
                                        match it with
                                        | :? Types.Row as r -> Some [ r ]
                                        | _ -> Some (getRowsRef env i.rows.Value)
                                    | _ -> None
                                return executeOnce rowsInOpt args'
                            finally
                                sem.Release() |> ignore
                        } ]
                tasks
                |> Task.WhenAll
                |> fun t -> t.Result
                |> Array.toList
                |> List.collect id

    let run (env:ExecEnv) =
        let sp = env.spec

        // 1) Execute steps (HookKey / DotNet / Exe + Parallel/Foreach)
        for node in sp.steps do
            match node with
            | Single st ->
                let rows = runOneStep env st
                env.stepRows.[st.id] <- rows

            | Parallel g ->
                let dop =
                    match g.parallelism with
                    | Some x when x > 1 -> x
                    | Some _ -> 1
                    | None -> 0 // 0 => unbounded
                if dop <= 1 then
                    // sequential within the group
                    for st in g.steps do
                        let rows = runOneStep env st
                        env.stepRows.[st.id] <- rows
                elif dop = 0 then
                    // unbounded concurrency
                    let tasks =
                        g.steps
                        |> List.map (fun st ->
                            task {
                                let rows = runOneStep env st
                                env.stepRows.[st.id] <- rows
                            })
                    Task.WhenAll(tasks) |> fun t -> t.Wait()
                else
                    // bounded concurrency with SemaphoreSlim
                    let sem = new SemaphoreSlim(dop)
                    let tasks =
                        g.steps
                        |> List.map (fun st ->
                            task {
                                do! sem.WaitAsync()
                                try
                                    let rows = runOneStep env st
                                    env.stepRows.[st.id] <- rows
                                finally
                                    sem.Release() |> ignore
                            })
                    Task.WhenAll(tasks) |> fun t -> t.Wait()

        // 2) Validations
        let errs =
            sp.validations
            |> List.choose (fun v ->
                let rows = getRowsRef env v.on
                Validate.evalRule v.id v.rule rows)
        if errs.Length > 0 then
            let msg =
                errs
                |> List.map (fun e -> $"{e.id}: {e.message}")
                |> String.concat "; "
            failwithf "Validation failed: %s" msg

        // 3) Materialize (pre_sql → write → post_sql)
        for m in sp.materialize do
            // resolve connection + table
            let tableName, connStr =
                match parseRef m.``to`` with
                | TableRef name ->
                    match sp.tables |> Map.tryFind name with
                    | Some t ->
                        let ds =
                            sp.datasources
                            |> Map.tryFind t.ds
                            |> Option.defaultWith (fun () -> failwithf "Datasource '%s' not found" t.ds)
                        t.name, ds.conn
                    | None -> failwithf "Unknown table ref '%s'" name
                | Literal s ->
                    let ds =
                        sp.datasources
                        |> Map.tryFind "default"
                        |> Option.defaultWith (fun () -> failwith "No default datasource")
                    s, ds.conn
                | _ -> failwithf "Invalid materialize.to: %s" m.``to``

            m.preSql  |> Option.iter env.sqlExec

            let srcRows = getRowsRef env m.from

            env.bulkEmit
                { conn  = connStr
                  table = tableName
                  mode  = m.mode
                  key   = m.key
                  rows  = srcRows }

            m.postSql |> Option.iter env.sqlExec

// -----------------------------
// Convenience + Scheduling
// -----------------------------
module Runner =
    open Types    open Parse
    open Exec2

    type Options =
        { overrideParams : Map<string,string>
          sqlExec        : Exec2.SqlExec
          bulkEmit       : Exec2.BulkEmit }

    let private applyOverrides (p:Params) (ov:Map<string,string>) =
        let g k d = ov |> Map.tryFind k |> Option.defaultValue d
        { envName  = g "envName"  p.envName
          dbName   = g "dbName"   p.dbName
          connStr  = g "connStr"  p.connStr
          hookNs   = g "hookNs"   p.hookNs
          impl     = g "impl"     p.impl
          batchId  = g "batchId"  p.batchId
          emitMode = g "emitMode" p.emitMode }

    let runFromYaml (yaml:string) (opt:Options) =
        let spec0 = Parse2.load yaml
        let spec1 = { spec0 with parameters = applyOverrides spec0.parameters opt.overrideParams }
        let spec  = Parse2.interpolateAll spec1
        
        
        

        let env =
            { Exec2.spec     = spec
              stepRows      = System.Collections.Concurrent.ConcurrentDictionary<string, Types.Rows>()
              sqlExec       = opt.sqlExec
              bulkEmit      = opt.bulkEmit }

        Exec2.run env
        env // return env if caller wants to inspect stepRows

    
