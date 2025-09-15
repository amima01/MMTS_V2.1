(*
# Batch (execute)
dotnet run --project src/Engine0 -- --run src/Engine0/workflows/S0_MMTS_ML.yaml

# Batch dry-run (no side effects)
dotnet run --project src/Engine0 -- --run src/Engine0/workflows/S0_MMTS_ML.yaml --dryrun

# Override params from CLI
dotnet run --project src/Engine0 -- --run src/Engine0/workflows/S0_MMTS_ML.yaml --set env=prod --set batchId=2025-09-15

# REPL passthrough (interactive)
dotnet run --project src/Engine0
*)
namespace Engine0

open System
open System.IO
open System.Collections.Generic

module Cli =
    type Mode =
        | Repl
        | Run of yamlPath:string * dryRun:bool * overrides:Map<string,string>

    let private trySplitKV (s:string) =
        let i = s.IndexOf('=')
        if i > 0 && i < s.Length-1 then
            let k = s.Substring(0, i).Trim()
            let v = s.Substring(i+1).Trim()
            if k <> "" then Some (k, v) else None
        else None

    /// Usage:
    ///   engine0 --run workflows\S0_MMTS_ML.yaml [--dryrun] [--set key=val ...]
    let parseArgs (argv:string[]) : Mode =
        if argv |> Array.exists (fun a -> a = "--run") then
            let mutable yaml = ""
            let mutable dryRun = false
            let kvs = Dictionary<string,string>(StringComparer.OrdinalIgnoreCase)

            let rec loop i =
                if i >= argv.Length then () else
                match argv.[i] with
                | "--run" when i+1 < argv.Length ->
                    yaml <- argv.[i+1]; loop (i+2)
                | "--dryrun" ->
                    dryRun <- true; loop (i+1)
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
                // FIX: convert Dictionary -> Map properly
                let overrides =
                    kvs
                    |> Seq.map (fun (KeyValue(k,v)) -> k, v)
                    |> Map.ofSeq
                Run(yaml, dryRun, overrides)
        else
            Repl

module Program =
    open Cli

    [<EntryPoint>]
    let main argv =
        try
            match parseArgs argv with
            | Repl ->
                printfn "Engine0 → REPL passthrough (MMTS.ML)."
                printfn "Commands: :load <file>, :list, :params, :set k=v, :hook show/set, :run [all|<step>], :validate [--stop-on-first-error], :emit, :dryrun emit, :save <file>, :q"
                MMTS.ML.Repl.start()

            | Run(yamlPath, dryRun, overrides) ->
                if not (File.Exists yamlPath) then
                    eprintfn "YAML not found: %s" yamlPath
                    2
                else
                    let yamlText = File.ReadAllText yamlPath
                    match MMTS.ML.Parse.tryParse yamlText with
                    | Error err ->
                        // FIX: don’t use `return` in a non-CE
                        eprintfn "Parse error: %s" err
                        3
                    | Ok spec ->
                        // Apply CLI overrides
                        let wf' = MMTS.ML.Exec.applyParamOverrides spec overrides

                        // Validate first
                        let errs = MMTS.ML.Exec.validate wf'
                        if not errs.IsEmpty then
                            eprintfn "Validation failed (%d):" errs.Length
                            errs |> List.iter (fun e -> eprintfn " - %s" e)
                            4
                        else
                            if dryRun then
                                printfn "DRYRUN: emitting plan for %s" (Path.GetFileName yamlPath)
                                MMTS.ML.Exec.emitDryRun wf' |> printfn "%s"
                                0
                            else
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
