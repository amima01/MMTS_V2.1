namespace MMTS.S0
open System

module S0_SqlOutputSetup =
    let mutable sqlArray : string array = Array.init 8 (fun _ -> "")

    sqlArray.[0] <- """
CREATE TABLE [dbo].[S0_Cadence31](
	[DataFeed] [nvarchar](20) NOT NULL,
	[Symbol] [nvarchar](32) NOT NULL,
	[StatName] [nvarchar](64) NOT NULL,
	[DataField] [nvarchar](16) NOT NULL,
	[TradeDate] [date] NOT NULL,
	[Dt] [datetime2](3) NOT NULL,
	[Raw] [float] NULL,
	[Med] [float] NULL,
	[Mad] [float] NULL,
	[Scale] [float] NULL,
	[MZ] [float] NULL,
	[Weight] [float] NULL,
	[Clean] [float] NULL,
	[Cadence] [float] NULL,
	[Source] [nvarchar](32) NULL,
	[InsertTime] [datetime2](3) NOT NULL,
 CONSTRAINT [PK_S0_Cadence31] PRIMARY KEY CLUSTERED 
(
	[DataFeed] ASC,
	[Symbol] ASC,
	[TradeDate] ASC,
	[Dt] ASC,
	[DataField] ASC,
	[StatName] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
"""
    sqlArray.[1] <- """
ALTER TABLE [dbo].[S0_Cadence31] ADD  DEFAULT (sysutcdatetime()) FOR [InsertTime]
    """

module S0_SqlSetup =
    let mutable sqlArray : string array = Array.init 8 (fun _ -> "")
    sqlArray.[0] <- """
-- TVP matches the subset we’ll write; other columns default NULL
IF TYPE_ID(N'dbo.S0_Cadence31Row') IS NOT NULL  
    DROP PROCEDURE S0_Cadence31_Upsert;    
"""
    sqlArray.[1] <- """
IF TYPE_ID(N'dbo.S0_Cadence31Row') IS NOT NULL  
    DROP TYPE dbo.S0_Cadence31Row;
"""
    sqlArray.[2] <- """
CREATE TYPE dbo.S0_Cadence31Row AS TABLE
(
  DataFeed    VARCHAR(32)  NOT NULL,
  Symbol      VARCHAR(32)  NOT NULL,
  StatName    VARCHAR(64)  NOT NULL,
  DataField   VARCHAR(64)  NULL,
  TradeDate   DATE         NOT NULL,
  Dt          DATETIME2(0) NOT NULL,
  Raw         FLOAT(53)    NULL,
  Med         FLOAT(53)    NULL,
  Mad         FLOAT(53)    NULL,
  Scale       FLOAT(53)    NULL,
  MZ          FLOAT(53)    NULL,
  Weight      FLOAT(53)    NULL,
  Clean       FLOAT(53)    NULL,
  Cadence     FLOAT(53)    NULL,
  Source      VARCHAR(64)  NULL
);
"""
    sqlArray.[3] <- """
CREATE OR ALTER PROCEDURE dbo.S0_Cadence31_Upsert
  @rows dbo.S0_Cadence31Row READONLY
AS
BEGIN
  SET NOCOUNT ON;

  DECLARE @rc TABLE (action nvarchar(10));

  MERGE dbo.S0_Cadence31 AS T
  USING @rows AS S
    ON  T.DataFeed  = S.DataFeed
    AND T.Symbol    = S.Symbol
    AND T.StatName  = S.StatName
    AND T.Dt        = S.Dt
    AND ISNULL(T.DataField,'') = ISNULL(S.DataField,'')
  WHEN MATCHED THEN
    UPDATE SET
      T.Raw     = COALESCE(S.Raw, T.Raw),
      T.Med     = COALESCE(S.Med, T.Med),
      T.Mad     = COALESCE(S.Mad, T.Mad),
      T.Scale   = COALESCE(S.Scale, T.Scale),
      T.MZ      = COALESCE(S.MZ, T.MZ),
      T.Weight  = COALESCE(S.Weight, T.Weight),
      T.Clean   = COALESCE(S.Clean, T.Clean),
      T.Cadence = COALESCE(S.Cadence, T.Cadence),
      T.Source  = COALESCE(S.Source, T.Source),
      T.TradeDate = S.TradeDate
  WHEN NOT MATCHED BY TARGET THEN
    INSERT (DataFeed,Symbol,StatName,DataField,TradeDate,Dt,Raw,Med,Mad,Scale,MZ,Weight,Clean,Cadence,Source)
    VALUES (S.DataFeed,S.Symbol,S.StatName,S.DataField,S.TradeDate,S.Dt,S.Raw,S.Med,S.Mad,S.Scale,S.MZ,S.Weight,S.Clean,S.Cadence,S.Source)
  OUTPUT $action INTO @rc;

  SELECT
    SUM(CASE WHEN action='INSERT' THEN 1 ELSE 0 END) AS inserted,
    SUM(CASE WHEN action='UPDATE' THEN 1 ELSE 0 END) AS updated,
    COUNT(*)                                         AS total
  FROM @rc;
END
"""
    sqlArray.[4] <- """
-- Upsert (MERGE) into S0_Cadence31
IF OBJECT_ID('dbo.S0_Cadence31_Upsert', 'P') IS NOT NULL
  DROP PROCEDURE dbo.S0_Cadence31_Upsert;
"""
    sqlArray.[5] <- """
CREATE PROCEDURE dbo.S0_Cadence31_Upsert
  @rows dbo.S0_Cadence31Row READONLY
AS
BEGIN
  SET NOCOUNT ON;

  MERGE dbo.S0_Cadence31 AS T
  USING @rows AS S
    ON  T.DataFeed  = S.DataFeed
    AND T.Symbol    = S.Symbol
    AND T.StatName  = S.StatName
    AND T.Dt        = S.Dt
    AND ISNULL(T.DataField,'') = ISNULL(S.DataField,'')
  WHEN MATCHED THEN
    UPDATE SET
      T.Raw       = COALESCE(S.Raw, T.Raw),
      T.Med       = COALESCE(S.Med, T.Med),
      T.Mad       = COALESCE(S.Mad, T.Mad),
      T.Scale     = COALESCE(S.Scale, T.Scale),
      T.MZ        = COALESCE(S.MZ, T.MZ),
      T.Weight    = COALESCE(S.Weight, T.Weight),
      T.Clean     = COALESCE(S.Clean, T.Clean),
      T.Cadence   = COALESCE(S.Cadence, T.Cadence),
      T.Source    = COALESCE(S.Source, T.Source),
      T.TradeDate = S.TradeDate
  WHEN NOT MATCHED BY TARGET THEN
    INSERT (DataFeed,Symbol,StatName,DataField,TradeDate,Dt,Raw,Med,Mad,Scale,MZ,Weight,Clean,Cadence,Source)
    VALUES (S.DataFeed,S.Symbol,S.StatName,S.DataField,S.TradeDate,S.Dt,S.Raw,S.Med,S.Mad,S.Scale,S.MZ,S.Weight,S.Clean,S.Cadence,S.Source);
END
"""
    sqlArray.[6] <- """
DROP INDEX IF EXISTS IX_S0_Cadence31_Key ON dbo.S0_Cadence31;
    """
    sqlArray.[7] <- """
CREATE INDEX IX_S0_Cadence31_Key ON dbo.S0_Cadence31(DataFeed,Symbol,StatName,Dt,DataField);
    """

