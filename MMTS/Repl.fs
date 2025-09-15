namespace MMTS.ML

open System
open System.IO
open System.Text
open System.Collections.Generic

module Repl =

    // ---- Domain expectations (from your MMTS.ML) ----
    // open MMTS.ML.Types
    // open MMTS.ML.Parse
    // open MMTS.ML.Exec

    type private State =
        { LoadedPath : string option
          Workflow   : obj option  // store as obj to avoid tight type coupling in this snippet
          Overrides  : Map<string,string>
          Dirty      : bool }
        static member Empty =
            { LoadedPath = None; Workflow = None; Overrides = Map.empty; Dirty = false }

    // Simple console helpers
    let private info (s:string)  = printfn "%s" s
    let private warn (s:string)  = eprintfn "%s" s
    let private error (s:string) = eprintfn "%s" s
    let private prompt() =
        if Console.IsOutputRedirected then () else
        Console.Write("> ")

    // (De)serialization for save/load (save stores the YAML we loaded + current overrides as a header)
    let private withApplied (wf: obj) (ovr: Map<string,string>) : obj =
        // Bridge to Exec.applyParamOverrides
        let wf' =
            try Exec.applyParamOverrides (wf :?> _) ovr :> obj
            with _ -> wf
        wf'

    // Try to summarize a workflow for :list / :params
    let private tryDescribe (wf: obj) : string =
        try
            let sb = StringBuilder()
            // naive reflection-based summary for minimal coupling

            // name/version
            let nameProp   = wf.GetType().GetProperty("Name")
            let verProp    = wf.GetType().GetProperty("Version")
            let nameStr    = if isNull nameProp then "(unknown)" else string (nameProp.GetValue wf)
            let verStr     = if isNull verProp then "(unknown)" else string (verProp.GetValue wf)
            sb.AppendLine($"workflow: {nameStr}  v{verStr}") |> ignore

            // params
            let paramProp  = wf.GetType().GetProperty("Params")
            if not (isNull paramProp) then
                match paramProp.GetValue wf with
                | :? IDictionary<string,string> as p ->
                    if p.Count = 0 then sb.AppendLine("params: (none)") |> ignore
                    else
                        sb.AppendLine("params:") |> ignore
                        for kv in p do sb.AppendLine($"  {kv.Key} = {kv.Value}") |> ignore
                | _ -> ()
            // steps
            let stepsProp = wf.GetType().GetProperty("Steps")
            if not (isNull stepsProp) then
                match stepsProp.GetValue wf with
                | :? IEnumerable<obj> as steps ->
                    let mutable c = 0
                    for s in steps do c <- c + 1
                    sb.AppendLine($"steps: {c}") |> ignore
                | _ -> ()
            sb.ToString()
        with _ ->
            "(uninspectable workflow)"

    let private showParams (wf: obj) =
        try
            let paramProp = wf.GetType().GetProperty("Params")
            match paramProp with
            | null -> info "no params on workflow"
            | p ->
                match p.GetValue wf with
                | :? IDictionary<string,string> as mm ->
                    if mm.Count = 0 then info "(no params)"
                    else
                        info "params:"
                        for kv in mm do info $"  {kv.Key} = {kv.Value}"
                | _ -> info "(params not a dictionary)"
        with ex ->
            warn $"cannot read params: {ex.Message}"

    let private showHooks (wf: obj) =
        try
            let hooksProp = wf.GetType().GetProperty("Hooks")
            match hooksProp with
            | null -> info "no hooks on workflow"
            | p ->
                match p.GetValue wf with
                | :? IDictionary<string,obj> as mm ->
                    if mm.Count = 0 then info "(no hooks)"
                    else
                        info "hooks:"
                        for kv in mm do
                            let name = kv.Key
                            let v = kv.Value
                            // Expecting fields: Module, Func, Kind (best-effort)
                            let modP = v.GetType().GetProperty("Module")
                            let funP = v.GetType().GetProperty("Func")
                            let kindP= v.GetType().GetProperty("Kind")
                            let m = if isNull modP then "" else string (modP.GetValue v)
                            let f = if isNull funP then "" else string (funP.GetValue v)
                            let k = if isNull kindP then "" else string (kindP.GetValue v)
                            info $"  {name} → module={m} func={f} kind={k}"
                | _ -> info "(hooks not a dictionary)"
        with ex ->
            warn $"cannot read hooks: {ex.Message}"

    let private loadFromFile (path:string) : Result<obj*string,string> =
        try
            if not (File.Exists path) then Error $"file not found: {path}"
            else
                let txt = File.ReadAllText path
                match Parse.tryParse txt with
                | Ok wf -> Ok (wf :> obj, txt)
                | Error e -> Error e
        with ex ->
            Error ex.Message

    let private saveToFile (path:string) (wf:obj) (ovr:Map<string,string>) =
        // we only save a dry-run emission so user sees the “resolved picture” plus overrides as a header
        try
            let wf' = withApplied wf ovr
            let rendered = Exec.emitDryRun (wf' :?> _) // string
            let sb = StringBuilder()
            sb.AppendLine("# MMTS.ML REPL save")
              .AppendLine("# overrides:")
              |> ignore
            if ovr.IsEmpty then sb.AppendLine("#   (none)") |> ignore
            else for KeyValue(k,v) in ovr do sb.AppendLine($"#   {k}={v}") |> ignore
            sb.AppendLine().Append(rendered) |> ignore
            File.WriteAllText(path, sb.ToString())
            Ok ()
        with ex -> Error ex.Message

    let private runValidate (wf:obj) =
        let errs = Exec.validate (wf :?> _)
        if errs.IsEmpty then info "validate: OK"
        else
            error $"validate: {errs.Length} error(s)"
            for e in errs do error $"  - {e}"

    let private runEmit (wf:obj) =
        let s = Exec.emitDryRun (wf :?> _)
        info s

    let private runAll (wf:obj) =
        match Exec.run (wf :?> _) with
        | Ok summary -> info $"OK: {summary}"; 0
        | Error err  -> error $"EXEC ERROR: {err}"; 1

    let private parseSet (s:string) =
        let i = s.IndexOf('=')
        if i > 0 && i < s.Length-1 then
            let k = s.Substring(0,i).Trim()
            let v = s.Substring(i+1).Trim()
            if k<>"" then Some(k,v) else None
        else None

    let private help() =
        info "MMTS ML REPL. Commands:"
        info "  :load <file>                load YAML"
        info "  :save <file>                save current (dry-run render + overrides header)"
        info "  :list                       show workflow + hooks summary"
        info "  :params                     show workflow params"
        info "  :set k=v                    set/override param k=v (runtime override)"
        info "  :unset k                    remove runtime override"
        info "  :validate [--stop-on-first-error]  validate workflow"
        info "  :emit                       pretty print dry-run execution plan"
        info "  :dryrun emit                alias of :emit"
        info "  :run                        execute whole workflow"
        info "  :q                          quit"

    /// Start the REPL; returns an exit code (0 = normal)
    let start () : int =
        let mutable st = State.Empty
        help()
        let mutable loop = true
        while loop do
            prompt()
            let line = Console.ReadLine()
            if isNull line then loop <- false else
            let cmd = line.Trim()
            if cmd = "" then ()
            else
                let parts = cmd.Split(' ', StringSplitOptions.RemoveEmptyEntries)
                match parts.[0] with
                | ":q" ->
                    loop <- false

                | ":help" | ":h" | "help" ->
                    help()

                | ":load" when parts.Length >= 2 ->
                    let path = String.Join(' ', parts |> Seq.skip 1)
                    match loadFromFile path with
                    | Ok (wf, _) ->
                        st <- { st with LoadedPath = Some path; Workflow = Some wf; Dirty = false }
                        info $"loaded: {path}"
                        info (tryDescribe wf)
                    | Error e -> error $"load failed: {e}"

                | ":save" when parts.Length >= 2 ->
                    match st.Workflow with
                    | None -> warn "nothing loaded"
                    | Some wf ->
                        let path = String.Join(' ', parts |> Seq.skip 1)
                        match saveToFile path wf st.Overrides with
                        | Ok () -> info $"saved: {path}"
                        | Error e -> error $"save failed: {e}"

                | ":list" ->
                    match st.Workflow with
                    | None -> warn "nothing loaded"
                    | Some wf ->
                        info (tryDescribe wf)
                        showHooks wf

                | ":params" ->
                    match st.Workflow with
                    | None -> warn "nothing loaded"
                    | Some wf -> showParams wf

                | ":set" when parts.Length >= 2 ->
                    let rest = String.Join(' ', parts |> Seq.skip 1)
                    match parseSet rest with
                    | Some (k,v) ->
                        st <- { st with Overrides = st.Overrides.Add(k,v); Dirty = true }
                        info $"override set: {k}={v}"
                    | None -> warn "usage: :set key=value"

                | ":unset" when parts.Length >= 2 ->
                    let k = parts.[1]
                    if st.Overrides.ContainsKey k then
                        st <- { st with Overrides = st.Overrides.Remove k; Dirty = true }
                        info $"override removed: {k}"
                    else warn $"no override named {k}"

                | ":validate" ->
                    match st.Workflow with
                    | None -> warn "nothing loaded"
                    | Some wf ->
                        let wf' = withApplied wf st.Overrides
                        runValidate wf'

                | ":emit" ->
                    match st.Workflow with
                    | None -> warn "nothing loaded"
                    | Some wf ->
                        let wf' = withApplied wf st.Overrides
                        runEmit wf'

                | ":dryrun" when parts.Length >= 2 && parts.[1] = "emit" ->
                    match st.Workflow with
                    | None -> warn "nothing loaded"
                    | Some wf ->
                        let wf' = withApplied wf st.Overrides
                        runEmit wf'

                | ":run" ->
                    match st.Workflow with
                    | None -> warn "nothing loaded"
                    | Some wf ->
                        let wf' = withApplied wf st.Overrides
                        let code = runAll wf'
                        if code <> 0 then () // keep session open, just report error

                | other ->
                    warn $"unknown command: {other}"
                    info "type :help for commands"

        0
