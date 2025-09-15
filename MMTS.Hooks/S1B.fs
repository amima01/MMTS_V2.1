namespace POC.Hooks.S1B.Emit.v1

open System
open System.Collections.Generic

module Module =

    type Row = IDictionary<string,obj>

    let private row (pairs: (string * obj) list) : Row =
        let d = Dictionary<string,obj>(StringComparer.OrdinalIgnoreCase) :> IDictionary<string,obj>
        for (k,v) in pairs do d.[k] <- v
        d

    let private tryGet (k: string) (m: IDictionary<string,string>) =
        match m.TryGetValue k with
        | true, v -> Some v
        | _ -> None

    /// Minimal alt-path emitter: produces signals with inverted scoring
    [<CompiledName("Run")>]
    let Run (args: IDictionary<string,string>) : obj =
        let env        = tryGet "env" args       |> Option.defaultValue "dev"
        let impl       = tryGet "impl" args      |> Option.defaultValue "v1"
        let dataFeed   = tryGet "dataFeed" args  |> Option.defaultValue "EOD5Secs"
        let signalName = tryGet "signalName" args|> Option.defaultValue "DOWNTREND_BETA"
        let batchId    = tryGet "batchId" args   |> Option.defaultValue (DateTime.Now.ToString("yyyy-MM-dd"))

        // Fixed trade date for repeatability
        let tradeDate =
            match DateTime.TryParse batchId with
            | true, dt -> DateTime(dt.Year, dt.Month, dt.Day, 15, 0, 0, DateTimeKind.Unspecified)
            | _        -> DateTime(2025, 01, 17, 15, 0, 0, DateTimeKind.Unspecified)

        let statName = tryGet "statName" args |> Option.defaultValue "WAP"

        // Hard-coded small universe
        let symbols = [ "AAPL"; "TSLA"; "NVDA" ]

        let rows = ResizeArray<Row>()

        for sym in symbols do
            // "alt" scoring: negative of hash mod 10
            let bump = -1.0 * (float (abs (sym.GetHashCode()) % 10) / 100.0)
            let score = bump
            let meta  = $"env={env};impl={impl};batchId={batchId};alt=1"
            rows.Add(
                row [
                    "DataFeed",   box dataFeed
                    "Symbol",     box sym
                    "StatName",   box statName
                    "TradeDate",  box tradeDate
                    "SignalName", box signalName
                    "Score",      box score
                    "Meta",       box meta
                ]
            )

        box rows
