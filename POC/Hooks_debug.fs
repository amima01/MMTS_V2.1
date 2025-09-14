namespace POC  

module ResultsDebug =
    open System
    open MMTS.ML.Types
    open POC.Log



    let private colsOf (rows: Rows) =
        match rows with
        | [] -> Set.empty
        | r::_ -> r |> Map.toList |> List.map fst |> Set.ofList

    let dumpSchemaDiff (stepId: string) (rows: Rows) (expected: string list) =
        let exp = Set.ofList expected
        let got = colsOf rows
        Log.infof "schema for step=%s" stepId
        Log.infof "  expected: %s" (String.Join(", ", expected))
        Log.infof "  got     : %s" (String.Join(", ", got |> Set.toList))
        let missing = exp - got
        let extra   = got - exp
        if not (Set.isEmpty missing) then Log.errorf "  MISSING: %s" (String.Join(", ", missing))
        if not (Set.isEmpty extra)   then Log.warnf  "  EXTRA  : %s" (String.Join(", ", extra))
    /// Pretty-print a set of step results (env.stepRows)
    let dumpResults (stepRows: System.Collections.Concurrent.ConcurrentDictionary<string, Rows>) =
        if stepRows.IsEmpty then
            Log.warn "results: <empty>"
        else
            Log.info "results summary:"
            for kv in stepRows |> Seq.sortBy (fun kv -> kv.Key) do
                let key  = kv.Key
                let rows = kv.Value
                Log.infof "step=%s rows=%d" key rows.Length

                if not rows.IsEmpty then
                    // take a sample row
                    let sample = rows |> List.head
                    let cols   = sample |> Map.toList |> List.map fst

                    // header
                    let header = String.Join(" | ", cols)
                    Log.infof "  cols: %s" header

                    // first few rows
                    rows
                    |> Seq.truncate 5
                    |> Seq.iter (fun r ->
                        let vals =
                            cols
                            |> List.map (fun c ->
                                match r.TryFind c with
                                | Some v -> string v
                                | None -> "")
                        Log.infof "  data: %s" (String.Join(" | ", vals)) )

                    if rows.Length > 5 then
                        Log.infof "  ... (%d more rows)" (rows.Length - 5)

module HooksDebug =
    open System
    open System.Reflection
    open MMTS.ML.Types
    open POC.Log

    // resolve a type by full name across loaded assemblies
    let private tryResolveType (fullName: string) =
        match Type.GetType(fullName, throwOnError = false) with
        | null ->
            AppDomain.CurrentDomain.GetAssemblies()
            |> Seq.tryPick (fun asm ->
                let t = asm.GetType(fullName, throwOnError = false)
                if isNull t then None else Some t)
        | t -> Some t

    /// Pretty-print the hooks table and validate type/method presence
    let dumpHooks (spec: Spec) =
        if spec.hooks.IsEmpty then
            Log.warn "hooks: <empty>"
        else
            // build rows
            let rows =
                spec.hooks
                |> Map.toList
                |> List.map (fun (key, h) ->
                    // status: TYPE? NOFUNC? OK
                    let status =
                        try
                            match tryResolveType h.moduleName with
                            | None      -> "TYPE?"
                            | Some typ  ->
                                let mi = typ.GetMethod(h.func, BindingFlags.Public ||| BindingFlags.Static)
                                if isNull mi then "NOFUNC" else "OK"
                        with _ -> "ERROR"
                    (key, h.moduleName, h.func, h.kind, status))

            // column widths
            let wKey   = max 3 (rows |> List.maxBy (fun (k,_,_,_,_) -> k.Length) |> fun (k,_,_,_,_) -> k.Length)
            let wMod   = max 6 (rows |> List.maxBy (fun (_,m,_,_,_) -> m.Length) |> fun (_,m,_,_,_) -> m.Length)
            let wFunc  = max 4 (rows |> List.maxBy (fun (_,_,f,_,_) -> f.Length) |> fun (_,_,f,_,_) -> f.Length)
            let wKind  = max 4 (rows |> List.maxBy (fun (_,_,_,k,_) -> k.Length) |> fun (_,_,_,k,_) -> k.Length)
            let wStat  = 6

            let keyHdr   = "key".PadRight wKey
            let modHdr   = "module".PadRight wMod
            let funcHdr  = "func".PadRight wFunc
            let kindHdr  = "kind".PadRight wKind
            let statHdr  = "status".PadRight wStat
            
            let header =
                $"| {keyHdr} | {modHdr} | {funcHdr} | {kindHdr} | {statHdr} |"
            let sep =
                "|-" + String('-', wKey) + "-|-" + String('-', wMod) + "-|-" + String('-', wFunc) +
                "-|-" + String('-', wKind) + "-|-" + String('-', wStat) + "-|"

            Log.info "hooks registry:"
            Log.info header
            Log.info sep
            rows
            |> List.iter (fun (k,m,f,kind,status) ->
                Log.info (sprintf "| %s | %s | %s | %s | %s |"
                              (k.PadRight wKey)
                              (m.PadRight wMod)
                              (f.PadRight wFunc)
                              (kind.PadRight wKind)
                              (status.PadRight wStat)))
