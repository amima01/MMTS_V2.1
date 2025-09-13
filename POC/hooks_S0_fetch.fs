namespace POC.Hooks.S0.Fetch.v1

open System
open MMTS.ML.Types

/// S0 Fetch: produces demo rows (Symbol, Price, Ts, BatchId)
/// Signature matches Exec.callDotNet when there is NO input rows:
///   inputs = [| box args |]
type Module =
    static member Run(args: Map<string,string>) : Rows =
        let symbol  = args |> Map.tryFind "item"    |> Option.defaultValue "DEMO"
        let batchId = args |> Map.tryFind "batchId" |> Option.defaultValue (DateTime.UtcNow.ToString("yyyy-MM-dd"))

        // produce a few sample rows
        [ for i in 1 .. 3 ->
            Map [
                "Symbol",   box symbol
                "Price",    box (100.0 + float i)
                "Ts",       box DateTime.UtcNow
                "BatchId",  box batchId
            ] ]
