namespace MMTS.S0

open System
open System.Data
open Microsoft.Data.SqlClient
open System.Globalization

[<CLIMutable>]
type Ohlc = { T: DateTime; O: float; H: float; L: float; C: float; V: float; Count: float; Wap: float }

type DataOrigin = | IBRK | YAHOO with
    override x.ToString() = match x with | IBRK -> "IBRK" | YAHOO -> "YAHOO"

type DataFeed = | EOD5Secs | EOD1Day_IBRK | EOD1Day_YAHOO  with
  override x.ToString() = match x with | EOD5Secs -> "EOD5Secs" | EOD1Day_IBRK -> "EOD1Day_IBRK" | EOD1Day_YAHOO -> "EOD1Day_YAHOO"

type DataField = OP | HI | LO | CL | WAP | VO | VOCO | Custom of string with
    override x.ToString() =
        match x with
        | OP -> "OP" | HI -> "HI" | LO -> "LO" | CL -> "CL"
        | WAP -> "WAP" | VO -> "VO" | VOCO -> "VOCO" | Custom s -> s
//type DataFeed = | EOD5Secs | EOD1Day with override x.ToString() = match x with | EOD5Secs -> "EOD5Secs" | EOD1Day -> "EOD1Day"

/// Map your column names once here and reuse everywhere
type ColumnMap = {
    DataFeedCol : string
    TimeCol   : string           // e.g. "DT" or "Time"
    OpenCol   : string           // e.g. "OP" or "Open"
    HighCol   : string           // e.g. "HI" or "High"
    LowCol    : string           // e.g. "LO" or "Low"
    CloseCol  : string           // e.g. "CL" or "Close"
    VolCol    : string    // e.g. Some "VO" or "Volume"; None if not stored
    SymbolCol : string           // e.g. "Symbol"
    CountCol : string
    WapCol : string
    Table     : string           // e.g. "[0_1Day_V02].[dbo].[EOD_1DAY]"
    //DataOriginCol : string 
}

/// One place to store sources for both feeds
type DbConfig = {
    Eod1Day  : ColumnMap
    Eod5Secs : ColumnMap
}


// -----------------------------------------------------------------------------
// Minimal SQL helper expected by your codebase (adjust if you already have one)
// -----------------------------------------------------------------------------
module Sql =
    let query (connStr:string) (sql:string) (map:SqlDataReader -> 'T) (ps:(string*obj) list) : 'T list =
        use conn = new SqlConnection(connStr)
        use cmd  = new SqlCommand(sql, conn)
        for (k,v) in ps do cmd.Parameters.AddWithValue(k, v) |> ignore
        conn.Open()
        use rd = cmd.ExecuteReader()
        let acc = ResizeArray<'T>()
        while rd.Read() do acc.Add (map rd)
        acc |> Seq.toList

    /// Read a DateTime by column name.
    /// - Accepts datetime/datetime2/date/smalldatetime (and will Convert if provider returns other types)
    /// - Throws if the value is NULL (so you notice unexpected nulls). Change if you prefer a default.
    let inline getDateTime (r: SqlDataReader) (name: string) : DateTime =
        let i = r.GetOrdinal(name)
        if r.IsDBNull(i) then
            invalidOp (sprintf "Column '%s' is NULL (expected DateTime)" name)
        else
            let ft = r.GetFieldType(i)
            if   ft = typeof<DateTime> then r.GetDateTime(i)
            else Convert.ToDateTime(r.GetValue(i), CultureInfo.InvariantCulture)

    /// Try-read a floating-point value by column name.
    /// - Returns NaN for NULLs or unparseable values
    /// - Handles SQL types: float/real/decimal/numeric/money/int/bigint/… and strings that parse as numbers
    let inline tryGetFloat (r: SqlDataReader) (name: string) : float =
        let i = r.GetOrdinal(name)
        if r.IsDBNull(i) then Double.NaN
        else
            match r.GetValue(i) with
            | :? double  as v -> v
            | :? single  as v -> float v
            | :? decimal as v -> float v
            | :? int16   as v -> float v
            | :? int32   as v -> float v
            | :? int64   as v -> float v
            | :? uint16  as v -> float v
            | :? uint32  as v -> float v
            | :? uint64  as v -> float v
            | :? string  as s ->
                let ok, v = Double.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture)
                if ok then v else Double.NaN
            | null -> Double.NaN
            | other -> Convert.ToDouble(other, CultureInfo.InvariantCulture)
module reader =
/// Read OHLC rows between [t0, t1] inclusive for a symbol from an arbitrary source (table + col map)
    let readOhlcGeneric
        (connStr: string)
        (src: ColumnMap)
        (dataFeed : string)
        (symbol: string)
        (t0: DateTime)
        (t1: DateTime)
        : Ohlc list =

        let sql = $"""
            SELECT                
                {src.TimeCol}   AS T,
                {src.OpenCol}   AS O,
                {src.HighCol}   AS H,
                {src.LowCol}    AS L,
                {src.CloseCol}  AS C,
                {src.VolCol}    AS V,
                {src.CountCol}    AS Count,
                {src.WapCol}    AS Wap
            FROM {src.Table}
            WHERE {src.SymbolCol} = @symbol
              AND {src.TimeCol} >= @t0
              AND {src.TimeCol} <= @t1
              AND {src.DataFeedCol} = @dataFeed              
            ORDER BY {src.TimeCol} ASC;
        """
        printfn "readOhlcGeneric: symbol=%s t0=%s t1=%s dataFeed=%s sql=%s" 
                symbol (t0.ToString("yyyy-MM-dd HH:mm:ss")) (t1.ToString("yyyy-MM-dd HH:mm:ss")) dataFeed sql

        use conn = new SqlConnection(connStr)
        use cmd  = new SqlCommand(sql, conn)
        cmd.Parameters.Add("@symbol", SqlDbType.NVarChar, 64).Value <- symbol :> obj
        cmd.Parameters.Add("@t0", SqlDbType.DateTime2).Value        <- t0     :> obj
        cmd.Parameters.Add("@t1", SqlDbType.DateTime2).Value        <- t1     :> obj
        cmd.Parameters.Add("@dataFeed", SqlDbType.NVarChar,64).Value      <- dataFeed     :> obj
        //cmd.Parameters.Add("@dataOrigin", SqlDbType.NVarChar,64).Value      <- src.DataOriginCol     :> obj
        conn.Open()
        use rd = cmd.ExecuteReader()

        let buf = System.Collections.Generic.List<Ohlc>(4096)
        while rd.Read() do
            let t = Sql.getDateTime rd "T"
            let o = Sql.tryGetFloat rd "O"
            let h = Sql.tryGetFloat rd "H"
            let l = Sql.tryGetFloat rd "L"
            let c = Sql.tryGetFloat rd "C"
            let v = Sql.tryGetFloat rd "V" |> fun x -> if Double.IsNaN x then 0.0 else x
            let count = Sql.tryGetFloat rd "Count"
            let wap = Sql.tryGetFloat rd "Wap"
            buf.Add({ T=t; O=o; H=h; L=l; C=c; V=v; Count=count ; Wap = wap})
        printfn "readOhlcGeneric buf.Count = %d" buf.Count
        List.ofSeq buf

    /// Convenience: choose the correct source by DataFeed
    let readOhlc
        (connStr: string)
        (cfg: DbConfig)
        (feed: DataFeed)
        (symbol: string)
        (t0: DateTime)
        (t1: DateTime)
        : Ohlc list =
        let src, feedName =
            match feed with
            | DataFeed.EOD1Day_YAHOO -> cfg.Eod1Day, "EOD1Day_YAHOO"
            | DataFeed.EOD1Day_IBRK  -> cfg.Eod1Day, "EOD1Day_IBRK"
            | DataFeed.EOD5Secs      -> cfg.Eod5Secs, "EOD5Secs"
            //| x -> failwithf "Unsupported DataFeed: %A" x
            //| _ -> failwithf "Unsupported DataFeed"
        readOhlcGeneric connStr src feedName symbol t0 t1
       

        
    

    


