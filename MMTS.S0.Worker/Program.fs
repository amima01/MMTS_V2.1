namespace MMTS.S0
open System
open System.Data
open Microsoft.Data.SqlClient
open reader

open OhlcChg
open S0Writer
open Robust
  // contains CadRow and writeCadence31
// ------------------------
    // Example config mappings
    // ------------------------

    module Examples =
        // Example for your known table:
        let eod1d : ColumnMap = {
            DataFeedCol = "DataFeed"
            TimeCol   = "DT"
            OpenCol   = "OP"
            HighCol   = "HI"
            LowCol    = "LO"
            CloseCol  = "CL"
            VolCol    = "VO"
            SymbolCol = "Symbol"
            CountCol = "Count"
            WapCol = "Wap"
            Table     = "[0_1Day_V01].[dbo].[EOD_1DAY]"
            //DataOriginCol =  "IBRK"
        }

        // Example guess for a 5-sec table; adjust names to your schema
        let eod5s : ColumnMap = {
            DataFeedCol = "DataFeed"
            TimeCol   = "DT"        // or "Time"
            OpenCol   = "OP"
            HighCol   = "HI"
            LowCol    = "LO"
            CloseCol  = "CL"
            VolCol    = "VO"
            SymbolCol = "Symbol"
            CountCol = "Count"
            WapCol = "Wap"
            Table     = "[0_1Day_V01].[dbo].[IBDATA_Last5Secs]"
            //DataOriginCol =  "IBRK"
        }

        let dbCfg : DbConfig = { Eod1Day = eod1d; Eod5Secs = eod5s }
module Program =
    [<EntryPoint>]
    
    let main _argv =
        let dbServer = "HP9"        
        let symbol = "AAPL"
        let connStr = sprintf "Server=%s;Database=0_1Day_V01;User Id=sa;Password=admin;Encrypt=False;TrustServerCertificate=True" dbServer

        //S0Cadence31Upsert.quickSmoke connStr
        /// Runs AAPL daily bars through:
        ///  1) OHLC: Cadence-31 over Close (StatName="CL", DataField="OHLC")
        ///  2) OHLC_CHG: chg_* features with Cadence-31 (DataField="OHLC_CHG")
        /// Writes both into dbo.S0_Cadence31.
        /// Returns (rowsWrittenOhlc, rowsWrittenChg).
        let runAaplDaily
            (connStr:string)
            (cfg: DbConfig)   // whatever your readOhlc needs; passed through
            (fromDate: DateTime)
            (toDate  : DateTime)
            : int * int =
            let mutable wroteOhlc_count = 0
            let mutable wroteOhlc_chg_count = 0
            for dataFeed in  [DataFeed.EOD1Day_YAHOO;  DataFeed.EOD1Day_IBRK; DataFeed.EOD5Secs ] do
                // 1) Load raw daily bars (assumes readOhlc is in scope)
                let bars1 =
                    readOhlc connStr cfg dataFeed symbol fromDate toDate
                printfn "bars1.Length = %d " bars1.Length
                // 2) Map into our Ohlc_chg type (pass Count getter if you have it)
            
                let bars_chg = toOhlcChgWithCountAndWap bars1 (fun b->b.T) (fun b->b.O) (fun b->b.H)
                                  (fun b->b.L) (fun b->b.C) (fun b->b.V)
                                  (fun b -> if Double.IsNaN b.Count then None else Some b.Count)
                                  (fun b -> if Double.IsNaN b.Wap   then None else Some b.Wap)


                printfn "bars_chg.Length = %d " bars_chg.Length

                // 3) Triples
                let triples = buildChangeTriples bars_chg
                printfn "triples.Length = %d " triples.Length
                
                // ---- A) OHLC path: Cadence-31 over Close ----

                let closeSeries_op = bars1 |> List.map (fun b -> b.T, b.O) |> List.sortBy fst
                let closeSeries_hi = bars1 |> List.map (fun b -> b.T, b.H) |> List.sortBy fst
                let closeSeries_lo = bars1 |> List.map (fun b -> b.T, b.L) |> List.sortBy fst
                let closeSeries_cl = bars1 |> List.map (fun b -> b.T, b.C) |> List.sortBy fst
                let closeSeries_vo = bars1 |> List.map (fun b -> b.T, b.V) |> List.sortBy fst
                let closeSeries_wap = bars1 |> List.map (fun b -> b.T, b.Wap) |> List.sortBy fst
                let closeSeries_count = bars1 |> List.map (fun b -> b.T, b.Count) |> List.sortBy fst
                
                let mutable series_items = [ "OP", closeSeries_op; "HI", closeSeries_hi; "LO", closeSeries_lo; "CL", 
                                            closeSeries_cl; "VO", closeSeries_vo; "WAP", closeSeries_wap] 
                if dataFeed = DataFeed.EOD1Day_IBRK then series_items <- series_items @ ["COUNT", closeSeries_count]
                for sname, series in series_items do
                    let enriched = RobustCadence31.cadence31 series
                    let ohlcRows : CadRow list =
                        enriched
                        |> List.map (fun (dt, raw, med, mad, scale, mz, w, clean, cad) ->
                            { DataFeed = dataFeed.ToString()
                              Symbol   = "AAPL"
                              StatName = sname
                              DataField= "OHLC"
                              TradeDate= dt.Date
                              Dt       = dt
                              Raw      = raw
                              Med      = med
                              Mad      = mad
                              Scale    = scale
                              MZ       = mz
                              Weight   = w
                              Clean    = clean
                              Cadence  = cad  // if CadRow uses float, change to cad
                              Source   = dataFeed.ToString() })

                    // Batch write OHLC rows
                    let wroteOhlc = writeCadence31 connStr ohlcRows
                    printfn "wroteOhlc = %d" wroteOhlc
                    wroteOhlc_count <- wroteOhlc_count + wroteOhlc

                (*
                // Then run Cadence-31 over WAP series and write:
                let wapSeries = buildWapSeries ohlc
                let wapEnriched = RobustCadence31.cadence31 wapSeries
                let wapRows : Robust.CadRow list =
                    wapEnriched
                    |> List.map (fun (dt, raw, med, mad, scale, mz, w, clean, cad) ->
                        { DataFeed="EOD1Day_IBRK"; Symbol="AAPL"; StatName="WAP"; DataField="OHLC"
                        TradeDate=dt.Date; Dt=dt; Raw=raw; Med=med; Mad=mad; Scale=scale
                        MZ=mz; Weight=w; Clean=clean; Cadence=cad; Source="IBKR_EOD" })
                let _ = Robust.writeCadence31 connStr wapRows
                *)

                // ---- B) OHLC_CHG path: build chg_* triples, then Cadence-31 per stat ----
                let triples = buildChangeTriples bars_chg
                // optional: restrict to specific chg_* names
                let keep = [ "chg_WAP"; "chg_CL"; "chg_OPxCL"; "chg_HI_LO"; "chg_OxHxLxC"; "chg_V"; "chg_OC_HL" ]

                // emit CadRow list ready for TVP upsert
                let rowsForUpsert =
                    Cadence31ChgWriter.cadence31_chg_toCadRows
                        triples
                        (dataFeed.ToString())
                        "AAPL"
                        "OHLC_CHG"
                        (dataFeed.ToString())
                        (Some keep)

                // write in one batch
                let wrote = Robust.writeCadence31 connStr rowsForUpsert
                wroteOhlc_chg_count <- wroteOhlc_chg_count + wrote
                printfn "Upserted %d Cadence-31 chg rows" wrote
                
            
            wroteOhlc_count, wroteOhlc_chg_count


        let cfg     = Examples.dbCfg
        let (nRow1, nRow2) = runAaplDaily connStr cfg (DateTime(2024,1,1)) (DateTime(2024,12,31))
        printfn "%d %d" nRow1 nRow2
        0 //end main