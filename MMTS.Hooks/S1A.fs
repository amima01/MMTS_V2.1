namespace POC.Hooks.S1A.Emit.v1

open System
open System.Collections.Generic

module Module =

    // Row shape expected by S1A table:
    // DataFeed (string)
    // Symbol   (string)
    // StatName (string)
    // TradeDate (DateTime)
    // SignalName (string)
    // Score    (float)
    // Meta     (string)  -- free-form json/text

    type Row = IDictionary<string,obj>

    let private row (pairs: (string * obj) list) : Row =
        let d = Dictionary<string,obj>(StringComparer.OrdinalIgnoreCase) :> IDictionary<string,obj>
        for (k,v) in pairs do d.[k] <- v
        d

    let private tryGet (k: string) (m: IDictionary<string,string>) =
        match m.TryGetValue k with
        | true, v -> Some v
        | _ -> None

    let private parseFloat (s: string option) (defaultValue: float) =
        match s with
        | Some x ->
            match Double.TryParse x with
            | true, v -> v
            | _ -> defaultValue
        | None -> defaultValue

    /// Minimal emitter: produces a few dummy signals
    /// Args (stringly-typed):
    ///   env, impl, batchId, dataFeed, signalName
    /// Optional knobs (for experimentation):
    ///   symbolList: "AAPL,MSFT,TSLA" (comma-separated)
    ///   baseScore: "1.0"
    [<CompiledName("Run")>]
    let Run (args: IDictionary<string,string>) : obj =
        let env        = tryGet "env" args       |> Option.defaultValue "dev"
        let impl       = tryGet "impl" args      |> Option.defaultValue "v1"
        let dataFeed   = tryGet "dataFeed" args  |> Option.defaultValue "EOD5Secs"
        let signalName = tryGet "signalName" args|> Option.defaultValue "UPSWING_ALPHA"
        let batchId    = tryGet "batchId" args   |> Option.defaultValue (DateTime.Now.ToString("yyyy-MM-dd"))

        // Optional comma-separated symbols; otherwise choose a tiny default set
        let symbols =
            match tryGet "symbolList" args with
            | Some s when not (String.IsNullOrWhiteSpace s) ->
                s.Split(',', StringSplitOptions.RemoveEmptyEntries) |> Array.toList |> List.map (fun x -> x.Trim().ToUpperInvariant())
            | _ -> [ "AAPL"; "MSFT" ]

        let baseScore = parseFloat (tryGet "baseScore" args) 1.0

        // Make a deterministic timestamp from batchId when possible
        let tradeDate =
            match DateTime.TryParse batchId with
            | true, dt -> DateTime(dt.Year, dt.Month, dt.Day, 15, 0, 0, DateTimeKind.Unspecified)
            | _        -> DateTime(2025, 01, 17, 15, 0, 0, DateTimeKind.Unspecified)

        // We’ll emit for a single StatName (WAP) for the demo; make it an arg if you like
        let statName = tryGet "statName" args |> Option.defaultValue "WAP"

        let rows = ResizeArray<Row>()

        // Tiny demo scoring function
        let scoreFor (sym: string) =
            // deterministic pseudo-signal: base + (symbol hash mod 10)/100
            let bump = (abs (sym.GetHashCode()) % 10) |> float |> fun x -> x / 100.0
            baseScore + bump

        for sym in symbols do
            let score = scoreFor sym
            let meta  = $"env={env};impl={impl};batchId={batchId}"
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
