namespace MMTS.S0

open System
open System.Data
open System.Numerics
open Microsoft.Data.SqlClient

type WeightKind = | Huber | Tukey

type WeightCfg = {
    Kind        : WeightKind   // Huber or Tukey (bisquare)
    C           : float        // cutoff (e.g., 1.345 for Huber, 4.685 for Tukey)
    UseVolume   : bool
    VolRefQ     : float        // reference quantile (e.g., 0.90 ⇒ p90 volume)
    VolPow      : float        // compression power for volume factor (e.g., 0.5)
    TimeTauSec  : float option // time-decay horizon in seconds (None to disable)
}

module utils =
    let field (f: DataField) (b: Ohlc) =
        match f with
        | OP -> b.O | HI -> b.H | LO -> b.L | CL -> b.C
        | WAP -> b.C      // swap to true WAP if available in Ohlc
        | VO -> b.V
        | VOCO -> if b.V > 0.0 then b.C / b.V else nan
        | Custom _ -> nan

type CadRow = {
    DataFeed  : string
    Symbol    : string
    StatName  : string 
    DataField : string
    TradeDate : DateTime
    Dt        : DateTime
    Raw       : float
    Med       : float
    Mad       : float
    Scale     : float
    MZ        : float
    Weight    : float
    Clean     : float
    Cadence   : float
    Source    : string 
}
module RobustCadence31 =
    open System

    let inline private medianOfSorted (xs: float[]) =
        let n = xs.Length
        if n = 0 then nan
        elif n % 2 = 1 then xs.[n/2]
        else (xs.[n/2 - 1] + xs.[n/2]) * 0.5

    let inline private median (xs: float[]) =
        let a = Array.copy xs
        Array.sortInPlace a
        medianOfSorted a

    let private eps = 1e-12

    /// Tukey biweight (c ≈ 4.685). Returns 0 for |z|>=c.
    let inline tukeyWeight (z: float) =
        let c = 4.685
        let a = abs z
        if a >= c then 0.0
        else
            let u = 1.0 - (a/c)*(a/c)
            u*u

    /// Compute Cadence-31 on a (Dt, Raw) dense series (ascending time).
    /// Returns list of (Dt, Raw, Med, Mad, Scale, MZ, Weight, Clean, Cadence)
    let cadence31 (series: (DateTime * float) list) =
        // maintain 31-window via simple sliding arrays
        let n = series.Length
        if n < 31 then [] else
        let rawArr = series |> List.map snd |> List.toArray
        let dtArr  = series |> List.map fst |> List.toArray

        let window = 31
        let half = 15

        let results = System.Collections.Generic.List<_>(n)
        // Pre-allocate window buffers (we copy each time for clarity; optimize later if needed)
        for i = 0 to n-1 do
            if i >= half && i+half < n then
                let s = i - half
                let e = i + half
                // slice raw[s..e] inclusive
                let win = rawArr.[s..e]
                let med = median win
                // MAD: median of absolute deviations
                let devs = win |> Array.map (fun v -> abs(v - med))
                let mad  = median devs
                let scale = max eps (1.4826 * mad)
                let raw = rawArr.[i]
                let mz  = (raw - med) / scale
                let w   = tukeyWeight mz
                let k = 3.0
                let clean = med + (max -k (min k mz)) * scale
                // Cadence: choose your favorite. For now, use mz (or use w*mz or EMA of mz)
                let cadence = mz
                results.Add (dtArr.[i], raw, med, mad, scale, mz, w, clean, cadence)
        results |> Seq.toList
    //--------------------------------------------------------
    // A nice record to carry name + cadence fields
    type CadChgPoint = { 
        Dt      : DateTime
        Name    : string
        Raw     : float
        Med     : float
        Mad     : float
        Scale   : float
        MZ      : float
        Weight  : float
        Clean   : float
        Cadence : float }

/// Compute Cadence-31 per chg_name (grouped)
    let cadence31_chg_grouped (series: (DateTime * string * float) list)
        : Map<string, CadChgPoint list> =
        
        // group rows by name
        series
        |> List.groupBy (fun (_,name,_) -> name)
        |> List.map (fun (name, rows) ->
            // sort this name's series by time and extract arrays
            let rowsSorted = rows |> List.sortBy (fun (t,_,_) -> t)
            let dtArr  = rowsSorted |> List.map (fun (t,_,_) -> t) |> List.toArray
            let rawArr = rowsSorted |> List.map (fun (_,_,v) -> v) |> List.toArray

            let n    = rawArr.Length
            let win  = 31
            let half = 15
            let out  = System.Collections.Generic.List<CadChgPoint>(max 0 n)

            if n >= win then
                for i = 0 to n-1 do
                    if i >= half && i + half < n then
                        let s = i - half
                        let e = i + half
                        let winSlice = rawArr.[s..e]
                        let med   = median winSlice
                        let devs  = winSlice |> Array.map (fun v -> abs (v - med))
                        let mad   = median devs
                        let scale = max eps (1.4826 * mad)
                        let raw   = rawArr.[i]
                        let mz    = (raw - med) / scale
                        let w     = tukeyWeight mz
                        let k     = 3.0
                        let clean = med + (max -k (min k mz)) * scale
                        let cadence = mz    // or pick w*mz / EMA(mz), etc.

                        out.Add {
                            Dt = dtArr.[i]; Name = name; Raw = raw; Med = med; Mad = mad
                            Scale = scale; MZ = mz; Weight = w; Clean = clean; Cadence = cadence
                        }
            name, (out |> Seq.toList))
        |> Map.ofList

    /// Same as above but flattened (easy to pipe into writers)
    let cadence31_chg_flat (series: (DateTime * string * float) list) : CadChgPoint list =
        cadence31_chg_grouped series
        |> Map.values
        |> Seq.collect id
        |> Seq.toList


    
module Robust =
    let inline median (xs: float array) =
        if xs.Length = 0 then nan
        else
            Array.sortInPlace xs
            if xs.Length % 2 = 1 then xs[xs.Length/2]
            else
                let i = xs.Length/2
                0.5 * (xs[i-1] + xs[i])

    /// centered rolling median + MAD (window = k, odd)
    let rollingMedMad (k: int) (ts: (DateTime*float) list) =
        if k < 1 || k % 2 = 0 then invalidArg "k" "window must be odd and >=1"
        let a = ts |> List.toArray
        let n = a.Length
        let half = k/2
        [|
            for i in 0 .. n-1 do
                let lo = max 0 (i-half)
                let hi = min (n-1) (i+half)
                let win = [| for j in lo .. hi do let _,v = a[j] in v |]
                let m  = median win
                let md = win |> Array.map (fun v -> abs (v - m)) |> median
                let t, v = a[i]
                yield t, v, m, md
        |] |> Array.toList
    // ADD this helper inside module Robust (above writeCadence31)
    let private toTvp (rows: CadRow list) =
        let dt = new DataTable()
        let add (n:string) (t:Type) = dt.Columns.Add(n, t) |> ignore
        add "DataFeed"  typeof<string>
        add "Symbol"    typeof<string>
        add "StatName"  typeof<string>
        add "DataField" typeof<string>
        add "TradeDate" typeof<DateTime>
        add "Dt"        typeof<DateTime>
        add "Raw"       typeof<float>
        add "Med"       typeof<float>
        add "Mad"       typeof<float>
        add "Scale"     typeof<float>
        add "MZ"        typeof<float>
        add "Weight"    typeof<float>
        add "Clean"     typeof<float>
        add "Cadence"   typeof<float>   // TVP expects float; we cast from your int
        add "Source"    typeof<string>
        for r in rows do
            let row = dt.NewRow()
            row.["DataFeed"]  <- r.DataFeed
            row.["Symbol"]    <- r.Symbol
            row.["StatName"]  <- r.StatName
            row.["DataField"] <- r.DataField //(if String.IsNullOrWhiteSpace r.DataField then null else r.DataField)
            row.["TradeDate"] <- r.TradeDate.Date
            row.["Dt"]        <- r.Dt
            row.["Raw"]       <- r.Raw
            row.["Med"]       <- r.Med
            row.["Mad"]       <- r.Mad
            row.["Scale"]     <- r.Scale
            row.["MZ"]        <- r.MZ
            row.["Weight"]    <- r.Weight
            row.["Clean"]     <- r.Clean
            row.["Cadence"]   <- float r.Cadence   // cast here
            row.["Source"]    <- r.Source //(if String.IsNullOrWhiteSpace r.Source then null else r.Source)
            dt.Rows.Add(row)
        dt
    let writeCadence31 (connStr: string) (rows: CadRow list) : int =
        if rows.IsEmpty then 0 else
        use conn = new SqlConnection(connStr)
        use cmd  = new SqlCommand("dbo.S0_Cadence31_Upsert", conn)
        cmd.CommandType <- CommandType.StoredProcedure
        let tvp = toTvp rows
        let p = cmd.Parameters.Add("@rows", System.Data.SqlDbType.Structured)
        p.TypeName <- "dbo.S0_Cadence31Row"
        p.Value    <- tvp
        conn.Open()
        cmd.ExecuteNonQuery()

module Cadence31ChgWriter =
    open System
    open RobustCadence31   // median, tukeyWeight, eps
    
    open Robust            // CadRow
    let private eps = 1e-12
    /// Compute Cadence-31 for each (Dt, chg_name, raw) series and emit CadRow list.
    /// - triples: (Dt, "chg_*", rawValue) across multiple change series
    /// - filterNames: if provided, only compute for these names
    /// - window=31 centered (15/15)
    /// - cadence = mz (swap if you prefer w*mz or EMA(mz))
    let cadence31_chg_toCadRows
        (triples      : (DateTime * string * float) list)
        (dataFeed     : string)
        (symbol       : string)
        (dataField    : string)
        (source       : string)
        (filterNames  : string list option)
        : CadRow list =
        
        // group rows by change name, optionally filter
        let grouped =
            triples
            |> List.groupBy (fun (_,name,_) -> name)
            |> fun g ->
                match filterNames with
                | None -> g
                | Some names ->
                    let want = Set names
                    g |> List.filter (fun (nm, _) -> want.Contains nm)

        // per-name computation
        let perName (name:string, rows:(DateTime*string*float) list) : CadRow list =
            let rowsSorted = rows |> List.sortBy (fun (t,_,_) -> t)
            let dtArr  = rowsSorted |> List.map (fun (t,_,_) -> t) |> List.toArray
            let rawArr = rowsSorted |> List.map (fun (_,_,v) -> v) |> List.toArray

            let n    = rawArr.Length
            let win  = 31
            let half = 15

            if n < win then
                []   // not enough points
            else
                let out = System.Collections.Generic.List<CadRow>(n)
                
                for i = 0 to n-1 do
                    if i >= half && i + half < n then
                        let s = i - half
                        let e = i + half
                        let slice = rawArr.[s..e]

                        let med   = median slice
                        let devs  = slice |> Array.map (fun v -> abs (v - med))
                        let mad   = median devs                        
                        let scale = max eps (1.4826 * mad)

                        let raw   = rawArr.[i]
                        let mz    = (raw - med) / scale
                        let w     = tukeyWeight mz

                        let k     = 3.0
                        let clean = med + (max -k (min k mz)) * scale

                        let cadence = mz   // choose your cadence summary here

                        let dt = dtArr.[i]
                        out.Add {
                            DataFeed  = dataFeed
                            Symbol    = symbol
                            StatName  = name
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
                            Cadence   = int cadence   // change CadRow to float if you prefer
                            Source    = source
                        }
                out |> Seq.toList

        grouped |> List.collect perName
module Stats =
    let inline median (xs: float array) =
        if xs.Length = 0 then nan
        else
            Array.sortInPlace xs
            if xs.Length % 2 = 1 then xs[xs.Length/2]
            else
                let i = xs.Length/2
                0.5 * (xs[i-1] + xs[i])

    let inline quantile (q: float) (xs: float array) =
        if xs.Length = 0 then nan
        else
            let x = Array.copy xs
            Array.sortInPlace x
            let q = min 1.0 (max 0.0 q)
            let p = q * float (x.Length - 1)
            let i = int (floor p)
            let f = p - float i
            if i >= x.Length - 1 then x[x.Length-1]
            else x[i] * (1.0 - f) + x[i+1] * f
module Weights =
    // robust weight from standardized residual r
    let robust (cfg: WeightCfg) (r: float) =
        match cfg.Kind with
        | Huber ->
            let a = abs r
            if a <= cfg.C then 1.0 else cfg.C / a
        | Tukey ->
            let u = r / cfg.C
            if abs u >= 1.0 then 0.0 else
            let oneMinusUSq = 1.0 - u*u
            oneMinusUSq * oneMinusUSq   // (1 - u^2)^2

    // volume factor in [0,1], relative to a per-day reference (e.g., p90)
    let volume (cfg: WeightCfg) (volRef: float) (v: float) =
        if not cfg.UseVolume || volRef <= 0.0 || v <= 0.0 then 1.0
        else
            // cap at 1.0 then soften with power
            let ratio = min 1.0 (v / volRef)
            ratio ** cfg.VolPow

    // time-decay factor in [0,1] from session midpoint (or use start if you prefer)
    let timeDecay (cfg: WeightCfg) (tauSecOpt: float option) (t: System.DateTime) (mid: System.DateTime) =
        match tauSecOpt with
        | None -> 1.0
        | Some tau ->
            let dtSec = abs (t - mid).TotalSeconds
            exp (- dtSec / tau)

    // combine factors
    let combine (wR: float) (wV: float) (wT: float) =
        // keep it in [0,1]
        let w = wR * wV * wT
        max 0.0 (min 1.0 w)
module Worker =
    type Cfg = {
        ConnStr   : string
        DataFeed  : DataFeed
        Cadence   : int            // 31
        Symbol    : string
        DataField : DataField
        StatName  : string   // Some "RSI_CL_5" if precomputed; None for pure OHLC/WAP
        Source    : DataOrigin   // "IBrk", "Yahoo", ...
        Bars      : Ohlc list      // already filtered to one TradeDate (or a range you chunk)
    }
    let defaultWeightCfg = {
        Kind       = Tukey     // or Huber
        C          = 4.685     // Tukey default; for Huber use ~1.345
        UseVolume  = true
        VolRefQ    = 0.90
        VolPow     = 0.5       // sqrt compression
        TimeTauSec = Some 3600. // 1 hour decay to 1/e from the day midpoint
    }
    let run (cfg: Cfg) =
        if List.isEmpty cfg.Bars then 0 else
        let ordered   = cfg.Bars |> List.sortBy (fun b -> b.T)
        let tradeDate = ordered.Head.T.Date

        // build the field series
        let series = ordered |> List.map (fun b -> b.T, utils.field cfg.DataField b)

        // rolling med/mad
        let medmad = Robust.rollingMedMad cfg.Cadence series

        // precompute per-day volume reference & time midpoint
        let vols = ordered |> List.map (fun b -> b.V) |> List.toArray
        let volRef =
            if vols.Length = 0 then nan
            else Stats.quantile defaultWeightCfg.VolRefQ vols

        // midpoint of the day's bars (you can switch to start-of-day if preferred)
        let tMid =
            let ts = ordered |> List.map (fun b -> b.T)
            let iMid = ts.Length / 2
            ts[iMid]

        // emit rows with weights
        let k = 1.4826  // normal-equivalent MAD scale
        let rows =
            List.zip ordered medmad
            |> List.map (fun (b, (t, raw, med, mad)) ->
                let scale = if mad > 0.0 then k * mad else 1.0
                let mz    = if scale > 0.0 then (raw - med) / scale else 0.0

                // factors
                let wR = Weights.robust defaultWeightCfg mz
                let wV = Weights.volume defaultWeightCfg volRef b.V
                let wT = Weights.timeDecay defaultWeightCfg defaultWeightCfg.TimeTauSec b.T tMid

                let w   = Weights.combine wR wV wT
                let cln = med  // keep as median baseline; or do med + clip(mz)*scale

                {
                    DataFeed  = cfg.DataFeed.ToString()
                    Symbol    = cfg.Symbol
                    StatName  = cfg.StatName
                    DataField = cfg.DataField.ToString()
                    TradeDate = b.T.Date
                    Dt        = b.T
                    Raw       = raw
                    Med       = med
                    Mad       = mad
                    Scale     = scale
                    MZ        = mz
                    Weight    = w
                    Clean     = cln
                    Cadence   = cfg.Cadence
                    Source    = cfg.Source.ToString()
                })

        
        let nrows = Robust.writeCadence31 cfg.ConnStr rows
        printfn "run : nrows=%d " nrows
        nrows

    let run2 (cfg: Cfg) : int =
        if List.isEmpty cfg.Bars then 0 else
        let ordered = cfg.Bars |> List.sortBy (fun b -> b.T)
        let tradeDate = ordered.Head.T.Date
        let series = ordered |> List.map (fun b -> b.T, utils.field cfg.DataField b)
        let medmad = Robust.rollingMedMad cfg.Cadence series

        // choose your robust scaling (k ~ 1.4826 for normal equivalence if needed)
        let k = 1.0
        let rows =
            List.zip ordered medmad
            |> List.map (fun (b, (t, raw, med, mad)) ->
                let scale = if mad > 0.0 then k * mad else 1.0
                let mz    = if scale > 0.0 then (raw - med) / scale else 0.0
                let w     = 1.0   // placeholder for your weighting scheme
                let clean = med   // or med + clip(mz, …) * scale, etc.
                {
                    DataFeed  = cfg.DataFeed.ToString()
                    Symbol    = cfg.Symbol
                    StatName  = cfg.StatName
                    DataField = cfg.DataField.ToString()
                    TradeDate = b.T.Date
                    Dt        = b.T
                    Raw       = raw
                    Med       = med
                    Mad       = mad
                    Scale     = scale
                    MZ        = mz
                    Weight    = w
                    Clean     = clean
                    Cadence   = cfg.Cadence
                    Source    = cfg.Source.ToString()
                })
        let nrows = Robust.writeCadence31 cfg.ConnStr rows
        printfn "run : nrows=%d " nrows
        nrows