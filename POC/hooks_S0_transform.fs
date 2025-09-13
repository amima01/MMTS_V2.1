namespace POC.Hooks.S0.Transform.v1

open System
open MMTS.ML.Types

/// S0 Transform: enriches rows (adds PriceZ and Impl)
/// Signature matches Exec.callDotNet when there ARE input rows:
///   inputs = [| box rows; box args |]
type Module =
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
