namespace POC.Hooks.S0.Transform.v1

open System
open MMTS.ML.Types

open MMTS.S0
open MMTS.S0.OhlcChg           // buildChangeTriples, toOhlcChgWithCountAndWap
open MMTS.S0.RobustCadence31   // cadence31 (OHLC), plus types for Cadence31-chg helpers
open MMTS.S0.Cadence31ChgWriter
open MMTS.S0.reader            // just for types; we don't hit the DB here

/// S0 Transform: enriches rows (adds PriceZ and Impl)
/// Signature matches Exec.callDotNet when there ARE input rows:
///   inputs = [| box rows; box args |]
type Module_test =
    static member Run(rows: Rows, args: Map<string,string>) : Rows =
        let impl = args |> Map.tryFind "impl" |> Option.defaultValue "default"

        rows
        |> List.map (fun r ->
            let price =
                match r |> Map.tryFind "Price" with
                | Some (:? IConvertible as v) -> v.ToDouble(Globalization.CultureInfo.InvariantCulture)
                | Some v -> failwithf "Price not numeric: %A" v
                | None   -> 0.0

            r
            |> Map.add "PriceZ" (box (price / 100.0))
            |> Map.add "Impl"   (box impl)
        )

[<AutoOpen>]
module private Util =
    let tryAs<'T> (k:string) (m:Row) =
        match Map.tryFind k m with
        | Some v ->
            try Some (v :?> 'T) with _ -> None
        | None -> None

    let getStr k (m:Row) = tryAs<string> k m |> Option.defaultValue ""
    let getF   k (m:Row) =
        match Map.tryFind k m with
        | Some (:? float as f) -> f
        | Some v ->
            match v with
            | :? int    as i -> float i
            | :? int64  as i -> float i
            | :? double as d -> d
            | :? single as s -> float s
            | :? decimal as d -> float d
            | _ -> Double.NaN
        | None -> Double.NaN

    let getDt k (m:Row) =
        match Map.tryFind k m with
        | Some (:? DateTime as t) -> t
        | Some v ->
            match v with
            | :? string as s ->
                let ok, dt = DateTime.TryParse s
                if ok then dt else DateTime.MinValue
            | _ -> DateTime.MinValue
        | None -> DateTime.MinValue

    /// Convert our robust CadRow record into pipeline Row (Map<string,obj>)
    let cadRowToRow (r: MMTS.S0.Robust.CadRow) : Row =
        Map [
          "DataFeed",  box r.DataFeed
          "Symbol",    box r.Symbol
          "StatName",  box r.StatName
          "DataField", box r.DataField
          "TradeDate", box r.TradeDate
          "Dt",        box r.Dt
          "Raw",       box r.Raw
          "Med",       box r.Med
          "Mad",       box r.Mad
          "Scale",     box r.Scale
          "MZ",        box r.MZ
          "Weight",    box r.Weight
          "Clean",     box r.Clean
          "Cadence",   box r.Cadence
          "Source",    box r.Source
        ]

    /// Build (Dt,value) series for a specific OHLC field
    let buildSeries (fieldName:string) (rows: Row list) : (DateTime * float) list =
        rows
        |> List.map (fun r ->
            let ts = getDt   "T" r
            let v  =
                match fieldName with
                | "OP"   -> getF "O" r
                | "HI"   -> getF "H" r
                | "LO"   -> getF "L" r
                | "CL"   -> getF "C" r
                | "VO"   -> getF "V" r
                | "WAP"  ->
                    let w = getF "Wap" r
                    if Double.IsNaN w then
                        // fallback: simple average if wap missing
                        let o = getF "O" r
                        let h = getF "H" r
                        let l = getF "L" r
                        let c = getF "C" r
                        (o + h + l + c) / 4.0
                    else w
                | "COUNT" -> getF "Count" r
                | _       -> Double.NaN
            ts, v)
        |> List.sortBy fst

    /// Derive Ohlc_chg list (with optional Count/Wap support) from input Rows
    let toOhlcChgList (rows: Row list) : OhlcChg.Ohlc_chg list =
        rows
        |> OhlcChg.toOhlcChgWithCountAndWap
               (fun r -> getDt "T" r)
               (fun r -> getF  "O" r)
               (fun r -> getF  "H" r)
               (fun r -> getF  "L" r)
               (fun r -> getF  "C" r)
               (fun r -> getF  "V" r)
               (fun r ->          // Count (optional)
                    let x = getF "Count" r
                    if Double.IsNaN x then None else Some x)
               (fun r ->          // Wap (optional)
                    let x = getF "Wap" r
                    if Double.IsNaN x then None else Some x)

    /// Project a group (feed,symbol) + one named series into CadRow list using cadence31
    let seriesToCadRows
        (feed:string)
        (symbol:string)
        (statName:string)
        (dataField:string)
        (source:string)
        (series: (DateTime*float) list)
        : MMTS.S0.Robust.CadRow list =
        RobustCadence31.cadence31 series
        |> List.map (fun (dt, raw, med, mad, scale, mz, w, clean, cad) ->
            { DataFeed  = feed
              Symbol    = symbol
              StatName  = statName
              DataField = dataField
              TradeDate = dt.Date
              Dt        = dt
              Raw       = raw
              Med       = med
              Mad       = mad
              Scale     = scale
              MZ        = mz
              Weight    = w
              Clean     = clean
              Cadence   = cad
              Source    = source })

type Module =
    /// Transform: takes OHLC rows from fetch and emits Cadence-31 rows (OHLC + chg_*).
    /// Args:
    ///   - mode: "both" (default) | "ohlc" | "chg"
    ///   - keep: optional CSV list of chg_* names to include (e.g., "chg_WAP,chg_CL")
    ///   - dataFieldOHLC: override DataField for OHLC rows (default "OHLC")
    ///   - dataFieldCHG : override DataField for chg rows  (default "OHLC_CHG")
    static member Run(rows: Rows, args: Map<string,string>) : Rows =
        // group by (DataFeed, Symbol) so mixed feeds/symbols work
        let groups =
            rows
            |> List.groupBy (fun r -> getStr "DataFeed" r, getStr "Symbol" r)

        let mode =
            args |> Map.tryFind "mode"
                 |> Option.defaultValue "both"
                 |> fun s -> s.Trim().ToLowerInvariant()

        let keepNamesOpt =
            args |> Map.tryFind "keep"
                 |> Option.map (fun csv ->
                        csv.Split([|','|], StringSplitOptions.RemoveEmptyEntries)
                        |> Array.map (fun s -> s.Trim())
                        |> Array.toList)

        let dfOHLC = args |> Map.tryFind "dataFieldOHLC" |> Option.defaultValue "OHLC"
        let dfCHG  = args |> Map.tryFind "dataFieldCHG"  |> Option.defaultValue "OHLC_CHG"

        let mutable outCadRows : MMTS.S0.Robust.CadRow list = []

        for ((feed,symbol), rowsOne) in groups do
            let sourceTag = if String.IsNullOrWhiteSpace feed then "UNKNOWN" else feed

            // ---- A) OHLC Cadence-31 ----
            if mode = "both" || mode = "ohlc" then
                // build series for OP/HI/LO/CL/VO/WAP (+COUNT if present)
                let series =
                    [ "OP"; "HI"; "LO"; "CL"; "VO"; "WAP" ]
                    |> List.map (fun name -> name, buildSeries name rowsOne)
                    |> List.filter (fun (_, s) -> not s.IsEmpty)

                // include COUNT only if present (any non-NaN)
                let hasCount =
                    rowsOne |> List.exists (fun r -> not (Double.IsNaN (getF "Count" r)))
                let series =
                    if hasCount then series @ [ "COUNT", buildSeries "COUNT" rowsOne ]
                    else series

                for (statName, s) in series do
                    let cad = Util.seriesToCadRows feed symbol statName dfOHLC sourceTag s
                    outCadRows <- outCadRows @ cad

            // ---- B) chg_* Cadence-31 ----
            if mode = "both" || mode = "chg" then
                let ohlcChg = toOhlcChgList rowsOne
                let triples = OhlcChg.buildChangeTriples ohlcChg
                let cadChgRows =
                    Cadence31ChgWriter.cadence31_chg_toCadRows
                        triples feed symbol dfCHG sourceTag keepNamesOpt
                outCadRows <- outCadRows @ cadChgRows

        // Convert to pipeline Rows and return
        outCadRows |> List.map cadRowToRow

    /// Overload: if the engine accidentally calls transform without input rows, be graceful.
    static member Run(args: Map<string,string>) : Rows =
        // No input rows ⇒ nothing to transform.
        []
