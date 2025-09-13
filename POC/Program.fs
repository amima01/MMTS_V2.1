namespace POC
open System
open YamlDotNet.RepresentationModel


module YamlDebug =

    let rec private dumpNode (indent:int) (node:YamlNode) =
        let pad = String(' ', indent)
        match node with
        | :? YamlScalarNode as s ->
            printfn "%s- SCALAR: %s" pad (if isNull s.Value then "<null>" else s.Value)
        | :? YamlMappingNode as m ->
            printfn "%s- MAPPING {" pad
            for kv in m.Children do
                let k = kv.Key :?> YamlScalarNode
                printfn "%s  Key: %s" pad k.Value
                dumpNode (indent+4) kv.Value
            printfn "%s}" pad
        | :? YamlSequenceNode as seq ->
            printfn "%s- SEQUENCE [" pad
            seq.Children
            |> Seq.iteri (fun i child ->
                printfn "%s  Item %d:" pad i
                dumpNode (indent+4) child )
            printfn "%s]" pad
        | other ->
            printfn "%s- UNKNOWN node type: %s" pad (other.GetType().FullName)

    /// Pretty-print entire YAML document tree
    let dumpYaml (yamlText:string) =
        let ys = YamlStream()
        ys.Load(new System.IO.StringReader(yamlText))
        let doc = ys.Documents.[0]
        printfn "ROOT DOCUMENT"
        dumpNode 0 doc.RootNode

module Program =
    open System
    open System.IO
    open System.Threading
    open System.Threading.Tasks
    open POC.Log
    open POC.Schedule
    
    open MMTS.ML.Types
    open MMTS.ML.Runner

    // ---------- helpers ----------
    let private previewFile (path:string) (maxLines:int) =
        try
            File.ReadLines(path)
            |> Seq.truncate maxLines
            |> Seq.mapi (fun i line -> sprintf "%3d| %s" (i+1) line)
            |> String.concat Environment.NewLine
        with _ -> "<unable to read file>"

    // ---------- one-shot YAML test runner with strong error handling ----------
    let private runYamlOnce (yamlPath: string) =
        try
            if not (File.Exists yamlPath) then
                Log.errorf "YAML not found at %s" (Path.GetFullPath yamlPath)
            else
                let yaml =
                    try 
                        //File.ReadAllText yamlPath
                        let yaml = File.ReadAllText yamlPath
                        YamlDebug.dumpYaml yaml
                        yaml
                    with ex ->
                        Log.errorf "Failed to read YAML: %s" ex.Message
                        raise ex

                // Show a small preview to help debugging structure problems
                Log.debugf "YAML preview (first 40 lines):%s%s"
                           Environment.NewLine
                           (previewFile yamlPath 40)


                let (env: MMTS.ML.Exec.ExecEnv)  =
                    try
                        // override a couple params so today's batch is used and hooks resolve to POC.Hooks
                        let overrides =
                            [ "batchId", DateTime.Today.ToString("yyyy-MM-dd")
                              "hookNs" , "POC.Hooks"
                              "impl"   , "v1" ]
                            |> Map.ofList

                        // stub side-effects
                        let sqlExec : MMTS.ML.Exec.SqlExec =
                            fun sql -> Log.infof "[sqlExec] %s" sql

                        let bulkEmit : MMTS.ML.Exec.BulkEmit =
                            fun args ->
                                let count = args.rows |> List.length
                                Log.infof "[bulkEmit] mode=%s table=%s rows=%d keys=%A conn=(%s)"
                                          args.mode args.table count args.key args.conn

                        // Parse + interpolate — catch YAML/shape errors cleanly
                        let spec0 =
                            try MMTS.ML.Parse.load yaml
                            with ex ->
                                Log.errorf "YAML parse failed: %s" ex.Message
                                // extra hint for common root shape errors
                                Log.warn  "Hint: top-level keys must be scalars (workflow/version/description/params/...)."
                                raise ex

                        let spec  = { 
                            spec0 with parameters = 
                                        let p = spec0.parameters
                                        let g (k:string) (d:string) = overrides |> Map.tryFind k |> Option.defaultValue d
                                        { p with batchId = g "batchId" p.batchId
                                                 hookNs  = g "hookNs"  p.hookNs
                                                 impl    = g "impl"    p.impl } }
                                    |> MMTS.ML.Parse.interpolateAll

                        // Execute via runner (this may raise on validation/materialize)
                        try
                            let (env1: MMTS.ML.Exec.ExecEnv)  = { 
                                    MMTS.ML.Exec.spec     = spec
                                    MMTS.ML.Exec.stepRows = System.Collections.Concurrent.ConcurrentDictionary<string, MMTS.ML.Types.Rows>()
                                    MMTS.ML.Exec.sqlExec  = sqlExec
                                    MMTS.ML.Exec.bulkEmit = bulkEmit }
                            MMTS.ML.Exec.run env1
                            env1
                                            
                        with ex ->
                            Log.errorf "Execution failed: %s" ex.Message
                            raise ex

                        
                    with ex ->
                        // Already logged; rethrow to let caller decide whether to continue
                        raise ex

                // summarize outputs if we got here
                env.stepRows
                |> Seq.map (fun kvp -> kvp.Key, kvp.Value.Length)
                |> Seq.sortBy fst
                |> Seq.iter (fun (k, n) -> Log.infof "[result] step=%s rows=%d" k n)

                env.stepRows
                |> Seq.iter (fun kvp ->
                    match kvp.Value with
                    | r::_ ->
                        let preview =
                            r
                            |> Map.toList
                            |> List.truncate 6
                            |> List.map (fun (k,v) -> $"{k}={v}")
                            |> String.concat "; "
                        Log.debugf "[peek] %s -> %s" kvp.Key preview
                    | [] -> ()
                )

        with ex ->
            // Final catch for anything unexpected
            Log.errorf "YAML run aborted: %s" ex.Message
            // Do not rethrow; allow app to continue (e.g., start scheduler)
            ()

    // ---------- scheduler ----------
    let private startScheduler () =
        let jobs : ScheduledJob list =
            [ ScheduledJob.ofTask
                  "heartbeat"
                  (Interval (TimeSpan.FromSeconds 10.0))
                  (fun () -> task {
                      Log.infof "Heartbeat at %O" DateTime.Now
                      return ()
                  }) ]
        Schedule.run jobs

    [<EntryPoint>]
    let main argv =
        Log.init "POC" "logs"

        // choose YAML path (allow override via --yaml "path")
        let yamlPath =
            let i = Array.FindIndex(argv, fun a -> a = "--yaml")
            if i >= 0 && i + 1 < argv.Length then argv.[i+1] else @"DOC\S0_MMTS_ML.yaml"

        let full = System.IO.Path.GetFullPath yamlPath
        Log.infof "Reading YAML from: %s" full
        
        

        Log.infof "Attempting YAML run: %s" (Path.GetFullPath yamlPath)
        runYamlOnce yamlPath

        // keep the app useful even if YAML failed
        startScheduler ()
        Console.WriteLine("Scheduler started. Press ENTER to exit.")
        Console.ReadLine() |> ignore

        Log.close()
        0
