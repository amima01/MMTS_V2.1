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
