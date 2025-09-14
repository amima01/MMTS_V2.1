namespace POC.Hooks.S0.Fetch.v1

open System
open MMTS.ML.Types

open System.Data
open System.Text
open Microsoft.Data.SqlClient
/// S0 Fetch: produces demo rows (Symbol, Price, Ts, BatchId)
/// Signature matches Exec.callDotNet when there is NO input rows:
///   inputs = [| box args |]
type Module_test =
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


open MMTS.ML.Types                  // Rows = list< Map<string,obj> >
open MMTS.S0                        // DataFeed, DataField, ColumnMap, DbConfig, etc.
open MMTS.S0.reader                 // readOhlc
open MMTS.S0.S0Writer               // (not used here, but handy if you later want to upsert raw)
//open MMTS.S0.S0_SqlSetup            // .sql text for TVP/proc/index install (your file)
open MMTS.S0.Robust                 // writeCadence31 (not used here)
open MMTS.S0                        // Examples.dbCfg lives in your uploaded Program.txt
open POC.Log
module private DbInit =

    /// Split a T-SQL script on GO batch separators (line with only 'GO', case-insensitive).
    let private splitOnGo (sql:string) =
        let lines = sql.Replace("\r\n","\n").Split('\n')
        let batches = System.Collections.Generic.List<string>()
        let sb = StringBuilder()
        let flush () =
            if sb.Length > 0 then
                batches.Add(sb.ToString().TrimEnd())
                sb.Clear() |> ignore
        for line in lines do
            if line.Trim().Equals("GO", StringComparison.OrdinalIgnoreCase) then
                flush()
            else
                sb.AppendLine(line) |> ignore
        flush()
        batches |> Seq.toList |> List.filter (fun b -> b.Trim() <> "")

    /// Execute a SQL statement (no results).
    let private execNonQuery (connStr:string) (sql:string) :int =

        //POC.Log.infof "[execNonQuery] connStr = %s  sql = %s" connStr sql 

        use conn = new SqlConnection(connStr)
        use cmd  = new SqlCommand(sql, conn)
        cmd.CommandType <- CommandType.Text
        conn.Open()
        let rc = cmd.ExecuteNonQuery() 
        //POC.Log.infof "[execNonQuery] sql = %s rc = %d" sql rc
        rc

    /// Create the S0_Cadence31 table if it doesn't exist (columns match your TVP & proc).
    let ensureCadence31Table (connStr:string) =
        let ddl = """
IF OBJECT_ID(N'dbo.S0_Cadence31', N'U') IS NULL
BEGIN
  CREATE TABLE dbo.S0_Cadence31
  (
    DataFeed   VARCHAR(32)  NOT NULL,
    Symbol     VARCHAR(32)  NOT NULL,
    StatName   VARCHAR(64)  NOT NULL,
    DataField  VARCHAR(64)  NULL,
    TradeDate  DATE         NOT NULL,
    Dt         DATETIME2(0) NOT NULL,
    Raw        FLOAT(53)    NULL,
    Med        FLOAT(53)    NULL,
    Mad        FLOAT(53)    NULL,
    Scale      FLOAT(53)    NULL,
    MZ         FLOAT(53)    NULL,
    Weight     FLOAT(53)    NULL,
    Clean      FLOAT(53)    NULL,
    Cadence    FLOAT(53)    NULL,
    Source     VARCHAR(64)  NULL
  );
END
"""
        //POC.Log.infof "[ensureCadence31Table] ddl = %s" ddl
        execNonQuery connStr ddl

    /// Install / refresh the TVP type + MERGE proc + index using your script (handles GO).
    let ensureTvpAndProc (connStr:string) =
        let batches = S0_SqlSetup.sqlArray //splitOnGo S0_SqlSetup.sql
        for b in batches do            
            let rc = execNonQuery connStr b
            POC.Log.infof "[ensureTvpAndProc] b = %s rc= %d" b rc

    /// One-shot idempotent install (safe to call every run)
    let ensureAll (connStr:string) =
        let rc = ensureCadence31Table connStr
        ensureTvpAndProc connStr


module private ParseArg =
    let get (k:string) (d:string) (m:Map<string,string>) =
        m |> Map.tryFind k |> Option.defaultValue d

    let getDate (k:string) (d:DateTime) (m:Map<string,string>) =
        match m |> Map.tryFind k with
        | None -> d
        | Some s ->
            match DateTime.TryParse s with
            | true, dt -> dt
            | _ -> d

    let tryFeed (s:string) =
        match s.Trim().ToUpperInvariant() with
        | "EOD1DAY_YAHOO" -> Some DataFeed.EOD1Day_YAHOO
        | "EOD1DAY_IBRK"  -> Some DataFeed.EOD1Day_IBRK
        | "EOD5SECS"      -> Some DataFeed.EOD5Secs
        | _ -> None

    /// Accept comma-separated list of feeds; default to EOD1Day_YAHOO
    let feeds (m:Map<string,string>) =
        match m |> Map.tryFind "feeds" with
        | None -> [ DataFeed.EOD1Day_YAHOO ]
        | Some csv ->
            csv.Split([|','|], StringSplitOptions.RemoveEmptyEntries)
            |> Array.choose (fun s -> tryFeed s)
            |> Array.toList
            |> function
               | [] -> [ DataFeed.EOD1Day_YAHOO ]
               | xs -> xs

open ParseArg
open DbInit

type Module =
    /// Run signature (no input rows): returns Rows for the next step.
    /// Args expected (defaults in parens):
    /// - connStr: SQL connection string (required in real runs)
    /// - symbol : ("AAPL")
    /// - t0     : from date ("2024-01-01")
    /// - t1     : to date   ("2024-12-31")
    /// - feeds  : CSV list of feeds ("EOD1Day_YAHOO,EOD1Day_IBRK")
    static member Run(args: Map<string,string>) : Rows =
        // 1) Connection string (try args, then a dev default)
        let connStr =
            match args |> Map.tryFind "connStr" with
            | Some s when not (String.IsNullOrWhiteSpace s) -> s
            | _ -> "Server=.;Database=0_1Day_V01;Trusted_Connection=True;Encrypt=False"
        
        POC.Log.infof "[POC.Hooks.S0.Fetch.v1.Run - pre ensureAll ] connStr = %s" connStr
              

        // 2) Ensure DB objects (table, TVP type, MERGE proc, index)
        ensureAll connStr  // uses your S0_SqlSetup.sql to install TVP/proc/index
        POC.Log.infof "[POC.Hooks.S0.Fetch.v1.Run - post ensureAll ] connStr = %s" connStr

        // 3) Pull request parameters
        let symbol = get "symbol" "AAPL" args
        let t0     = getDate "t0" (DateTime(2024,1,1)) args
        let t1     = getDate "t1" (DateTime(2024,12,31)) args
        let feedsL = feeds args

        // 4) Use your Example mappings for EOD1Day/EOD5Secs
        //    (from your Program.txt: Examples.dbCfg)
        let dbCfg = MMTS.S0.Examples.dbCfg
            // MMTS.S0.Examples.dbCfg was included with your upload; reference it here.
            // If you later move it, inject via params instead.
            

        // 5) Read bars for each feed, accumulate into Rows
        let mutable out : Rows = []

        let feed_connStr = "Server=HP9;Database=0_1Day_V01;User Id=sa;Password=admin;Encrypt=False;TrustServerCertificate=True"
        for feed in feedsL do
            let bars = reader.readOhlc feed_connStr dbCfg feed symbol t0 t1
            // Map each OHLC row into a generic Row (Map<string,obj>) for MMTS
            let feedName = feed.ToString()
            let rows =
                bars
                |> List.map (fun b ->
                    Map [
                        "DataFeed",  box feedName
                        "Symbol",    box symbol
                        "T",         box b.T
                        "O",         box b.O
                        "H",         box b.H
                        "L",         box b.L
                        "C",         box b.C
                        "V",         box b.V
                        "Count",     box b.Count
                        "Wap",       box b.Wap
                    ])
            out <- out @ rows
            POC.Log.infof "[POC.Hooks.S0.Fetch.v1.Run] feed_connStr = %s feed = %s out.Length=%d" feed_connStr feedName out.Length

        // 6) Return combined Rows to the pipeline (e.g., for S0.Transform)
        out
