namespace Engine1A

open System
open System.IO
open System.Collections.Generic

module Cli =
    type Mode =
        | Repl
        | Run of yamlPath:string * dryRun:bool * stepId:string option * overrides:Map<string,string>

    let private trySplitKV (s:string) =
        let i = s.IndexOf('=')
        if i > 0 && i < s.Length-1 then
            let k = s.Substring(0, i).Trim()
            let v = s.Substring(i+1).Trim()
            if k <> "" then Some (k, v) else None
        else None

    /// Usage:
    ///   engine1a --run workflows\S1A_MMTS_ML.yaml [--dryrun] [--step <stepId>] [--set key=val ...]
    let parseArgs (argv:string[]) : Mode =
        if argv |> Array.exists (fun a -> a = "--run") then
            let mutable yaml = ""
            let mutable dryRun = false
            let mutable stepId : string option = None
            let kvs = Dictionary<string,string>(StringComparer.OrdinalIgnoreCase)

            let rec loop i =
                if i >= argv.Length then () else
                match argv.[i] with
                | "--run" when i+1 < argv.Length ->
                    yaml <- argv.[i+1]; loop (i+2)
                | "--dryrun" ->
                    dryRun <- true; loop (i+1)
                | "--step" when i+1 < argv.Length ->
                    stepId <- Some argv.[i+1]; loop (i+2)
                | "--set" when i+1 < argv.Length ->
                    match trySplitKV argv.[i+1] with
                    | Some (k,v) -> kvs.[k] <- v
                    | None -> ()
                    loop (i+2)
                | _ ->
                    loop (i+1)
            loop 0

            if String.IsNullOrWhiteSpace yaml then Repl
            else
                let overrides =
                    kvs |> Seq.map (fun (KeyValue(k,v)) -> k, v) |> Map.ofSeq
                Run(yaml, dryRun, stepId, overrides)
        else
            Repl

module Program =
    open Cli

    [<EntryPoint>]
    let main argv =
        try
            match parseArgs argv with
            | Repl ->
                printfn "Engine1A → REPL passthrough (MMTS.ML)."
                printfn "Commands: :load <file>, :list, :params, :set k=v, :hook show/set, :run [all|<step>], :validate [--stop-on-first-error], :emit, :dryrun emit, :save <file>, :q"
                MMTS.ML.Repl.start()

            | Run(yamlPath, dryRun, stepIdOpt, overrides) ->
                if not (File.Exists yamlPath) then
                    eprintfn "YAML not found: %s" yamlPath
                    2
                else
                    let yamlText = File.ReadAllText yamlPath
                    match MMTS.ML.Parse.tryParse yamlText with
                    | Error err ->
                        eprintfn "Parse error: %s" err
                        3
                    | Ok spec ->
                        let wf' = MMTS.ML.Exec.applyParamOverrides spec overrides

                        let errs = MMTS.ML.Exec.validate wf'
                        if not errs.IsEmpty then
                            eprintfn "Validation failed (%d):" errs.Length
                            errs |> List.iter (fun e -> eprintfn " - %s" e)
                            4
                        else
                            match dryRun, stepIdOpt with
                            | true, _ ->
                                printfn "DRYRUN: emitting plan for %s" (Path.GetFileName yamlPath)
                                MMTS.ML.Exec.emitDryRun wf' |> printfn "%s"
                                0
                            | false, Some stepId ->
                                // Run up to a specific step (requires Exec.runStep in MMTS.ML)
                                printfn "RUN (step): %s → %s" (Path.GetFileName yamlPath) stepId
                                match MMTS.ML.Exec.runStep wf' stepId with
                                | Ok summary ->
                                    printfn "OK: %s" summary
                                    0
                                | Error err ->
                                    eprintfn "EXEC ERROR: %s" err
                                    5
                            | false, None ->
                                // Run whole workflow
                                printfn "RUN: executing %s" (Path.GetFileName yamlPath)
                                match MMTS.ML.Exec.run wf' with
                                | Ok summary ->
                                    printfn "OK: %s" summary
                                    0
                                | Error err ->
                                    eprintfn "EXEC ERROR: %s" err
                                    5
        with ex ->
            eprintfn "FATAL: %s" (ex.ToString())
            1
