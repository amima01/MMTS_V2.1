namespace MMTS.S0
open System
open System.Data
open Microsoft.Data.SqlClient
//namespace MMTS.S2.Tests


open Robust   // <- your module containing type CadRow and writeCadence31

module S0Cadence31Upsert =

    // --- basic assert helpers (no external framework) ---
    // --- basic assert helpers (no external framework) ---
    let private assertEq name expected actual =
        if expected <> actual then
            failwithf "AssertEq failed [%s]: expected=%A actual=%A" name expected actual

    let private assertClose name (eps:float) (expected:float) (actual:float) =
        if Double.IsNaN(expected) && Double.IsNaN(actual) then () else
        if abs (expected - actual) > eps then
            failwithf "AssertClose failed [%s]: expected=%f actual=%f (eps=%f)" name expected actual eps
    (*
    let private assertEq name expected actual =
        if not (obj.Equals(expected, actual)) then
            failwithf "AssertEq failed [%s]: expected=%A actual=%A" name expected actual

    let private assertClose name (eps:float) (expected:float) (actual:float) =
        if Double.IsNaN(expected) && Double.IsNaN(actual) then () else
        if abs (expected - actual) > eps then
            failwithf "AssertClose failed [%s]: expected=%f actual=%f (eps=%f)" name expected actual eps
    *)
    // --- SELECT helper to fetch back rows we just wrote ---
    type S0Row =
        { DataFeed  : string
          Symbol    : string
          StatName  : string
          DataField : string
          Dt        : DateTime
          Raw       : float
          Med       : float
          Mad       : float
          Scale     : float
          MZ        : float
          Weight    : float
          Clean     : float
          Cadence   : float
          Source    : string }

    let private fetchRows (connStr:string) (dataFeed:string) (symbol:string) (statName:string) (dataField:string) =
        use conn = new SqlConnection(connStr)
        use cmd  = new SqlCommand("""
            SELECT DataFeed,Symbol,StatName,ISNULL(DataField,''),Dt,
                   ISNULL(Raw,0),ISNULL(Med,0),ISNULL(Mad,0),ISNULL(Scale,0),
                   ISNULL(MZ,0),ISNULL(Weight,0),ISNULL(Clean,0),ISNULL(Cadence,0),
                   ISNULL(Source,'')
            FROM dbo.S0_Cadence31 WITH (READCOMMITTED)
            WHERE DataFeed=@df AND Symbol=@sym AND StatName=@sn AND ISNULL(DataField,'') = @dfld
            ORDER BY Dt ASC;
        """, conn)
        cmd.Parameters.Add("@df",   SqlDbType.VarChar, 32).Value <- dataFeed :> obj
        cmd.Parameters.Add("@sym",  SqlDbType.VarChar, 64).Value <- symbol   :> obj
        cmd.Parameters.Add("@sn",   SqlDbType.VarChar, 64).Value <- statName :> obj
        cmd.Parameters.Add("@dfld", SqlDbType.VarChar, 64).Value <- dataField:> obj
        conn.Open()
        use rd = cmd.ExecuteReader(CommandBehavior.SequentialAccess)
        let rows = System.Collections.Generic.List<S0Row>()
        while rd.Read() do
            rows.Add ({
                DataFeed  = rd.GetString(0)
                Symbol    = rd.GetString(1)
                StatName  = rd.GetString(2)
                DataField = rd.GetString(3)
                Dt        = rd.GetDateTime(4)
                Raw       = rd.GetDouble(5)
                Med       = rd.GetDouble(6)
                Mad       = rd.GetDouble(7)
                Scale     = rd.GetDouble(8)
                MZ        = rd.GetDouble(9)
                Weight    = rd.GetDouble(10)
                Clean     = rd.GetDouble(11)
                Cadence   = rd.GetDouble(12)
                Source    = rd.GetString(13)
            })
        rows |> Seq.toList

    /// Quick end-to-end smoke test for Robust.writeCadence31
    /// - Creates a unique symbol to avoid clobbering real data
    /// - Writes 2 rows, verifies insert
    /// - Writes an update for the 2nd row, verifies MERGE update
    let quickSmoke (connStr:string) =
        let dataFeed  = "TEST_DF"
        let statName  = "chg_CL"
        let dataField = "OHLC_CHG"
        let symbol    = "UTEST_" + Guid.NewGuid().ToString("N").Substring(0,12)
        let src       = "unit_test"

        // two timestamps (same trade date for convenience)
        let t0 = DateTime(2025, 01, 02, 12, 00, 00, DateTimeKind.Utc)
        let t1 = DateTime(2025, 01, 02, 12, 05, 00, DateTimeKind.Utc)

        // rows v1 (initial insert)
        let rowsV1 : CadRow list = [
            { DataFeed=dataFeed; Symbol=symbol; StatName=statName; DataField=dataField
              TradeDate=t0.Date; Dt=t0; Raw=100.0; Med=100.0; Mad=1.0; Scale=1.4826; MZ=0.0
              Weight=1.0; Clean=100.0; Cadence=0; Source=src }
            { DataFeed=dataFeed; Symbol=symbol; StatName=statName; DataField=dataField
              TradeDate=t1.Date; Dt=t1; Raw=101.0; Med=100.5; Mad=1.1; Scale=1.63; MZ=0.3
              Weight=0.9; Clean=100.8; Cadence=1; Source=src }
        ]

        // write v1
        let _ = Robust.writeCadence31 connStr rowsV1

        // verify insert
        let got1 = fetchRows connStr dataFeed symbol statName dataField
        assertEq "count v1" 2 got1.Length
        assertClose "row0 raw v1" 100.0 got1.[0].Raw 1e-9
        assertClose "row1 raw v1" 101.0 got1.[1].Raw 1e-9
        assertClose "row1 scale v1" 1.63 got1.[1].Scale 1e-9
        assertEq   "row1 cadence v1" 1.0 got1.[1].Cadence   // your writer casts int->float

        // rows v2 (update t1 only; change Raw and Clean)
        let rowsV2 : CadRow list = [
            { DataFeed=dataFeed; Symbol=symbol; StatName=statName; DataField=dataField
              TradeDate=t1.Date; Dt=t1; Raw=102.5; Med=100.7; Mad=1.2; Scale=1.70; MZ=0.5
              Weight=0.85; Clean=101.2; Cadence=2; Source=src }
        ]

        let _ = Robust.writeCadence31 connStr rowsV2

        // verify merge update (t0 unchanged, t1 updated)
        let got2 = fetchRows connStr dataFeed symbol statName dataField
        assertEq "count v2" 2 got2.Length
        assertClose "row0 raw v2 (unchanged)" 100.0 got2.[0].Raw 1e-9
        assertClose "row1 raw v2 (updated)"   102.5 got2.[1].Raw 1e-9
        assertClose "row1 clean v2 (updated)" 101.2 got2.[1].Clean 1e-9
        assertEq   "row1 cadence v2 (updated)" 2.0 got2.[1].Cadence

        printfn "✅ S0_Cadence31 TVP upsert smoke test passed for %s / %s" dataFeed symbol
