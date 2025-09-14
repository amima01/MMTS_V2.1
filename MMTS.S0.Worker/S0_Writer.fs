namespace MMTS.S0
open System
open System.Data
open Microsoft.Data.SqlClient

module S0Writer =

    // Build a DataTable matching dbo.S0_Cadence31Row TVP
    let private toTvp (rows: seq<string*string*string*string option*DateTime*DateTime*float*string option>) =
        // row shape: DataFeed, Symbol, StatName, DataField?, TradeDate, Dt, Raw, Source?
        let dt = new DataTable()
        dt.Columns.Add("DataFeed",  typeof<string>)   |> ignore
        dt.Columns.Add("Symbol",    typeof<string>)   |> ignore
        dt.Columns.Add("StatName",  typeof<string>)   |> ignore
        dt.Columns.Add("DataField", typeof<string>)   |> ignore
        dt.Columns.Add("TradeDate", typeof<DateTime>) |> ignore
        dt.Columns.Add("Dt",        typeof<DateTime>) |> ignore
        dt.Columns.Add("Raw",       typeof<float>)    |> ignore
        dt.Columns.Add("Med",       typeof<float>)    |> ignore
        dt.Columns.Add("Mad",       typeof<float>)    |> ignore
        dt.Columns.Add("Scale",     typeof<float>)    |> ignore
        dt.Columns.Add("MZ",        typeof<float>)    |> ignore
        dt.Columns.Add("Weight",    typeof<float>)    |> ignore
        dt.Columns.Add("Clean",     typeof<float>)    |> ignore
        dt.Columns.Add("Cadence",   typeof<float>)    |> ignore
        dt.Columns.Add("Source",    typeof<string>)   |> ignore

        for (dataFeed,symbol,statName,dataFieldOpt,tradeDate,dtm,rawVal,sourceOpt) in rows do
            let row = dt.NewRow()
            row.["DataFeed"]  <- dataFeed
            row.["Symbol"]    <- symbol
            row.["StatName"]  <- statName
            row.["DataField"] <- (match dataFieldOpt with Some s -> s | None -> null)
            row.["TradeDate"] <- tradeDate.Date
            row.["Dt"]        <- dtm
            row.["Raw"]       <- rawVal
            // leave the rest NULL
            row.["Med"]       <- DBNull.Value
            row.["Mad"]       <- DBNull.Value
            row.["Scale"]     <- DBNull.Value
            row.["MZ"]        <- DBNull.Value
            row.["Weight"]    <- DBNull.Value
            row.["Clean"]     <- DBNull.Value
            row.["Cadence"]   <- DBNull.Value
            row.["Source"]    <- (match sourceOpt with Some s -> s | None -> null)
            dt.Rows.Add(row)
        dt

    /// Upsert change-series (e.g., chg_WAP, chg_CL, ...) into dbo.S0_Cadence31
    /// - `triples`: (Dt, StatName, RawValue)
    /// - Writes with DataField="OHLC_CHG" (override via ?dataField)
    /// - Source can be stamped like "IBKR_EOD"
    let upsertChanges
        (connStr: string)
        (dataFeed: string)
        (symbol: string)
        (triples: (DateTime * string * float) list)
        (dataField: string )
        (sourceTag: string )
        =
        if triples.IsEmpty then 0 else
        let df : string option = Some dataField //defaultArg dataField "OHLC_CHG"
        let src: string option = Some sourceTag //defaultArg sourceTag (sprintf "%s_changes" dataFeed)

        // Map triples to TVP rows
        let tvpRows =
            triples
            |> List.map (fun (dt, statName, value) ->
                let tradeDate = dt.Date
                (dataFeed, symbol, statName, df, tradeDate, dt, value, src))

        use conn = new SqlConnection(connStr)
        use cmd  = new SqlCommand("dbo.S0_Cadence31_Upsert", conn)
        cmd.CommandType <- CommandType.StoredProcedure

        let tvp = toTvp tvpRows
        let p = cmd.Parameters.Add("@rows", SqlDbType.Structured)
        p.TypeName <- "dbo.S0_Cadence31Row"
        p.Value    <- tvp

        conn.Open()
        cmd.ExecuteNonQuery()


