

namespace MMTS.S0

open System

module OhlcChg =

    // ------------- Types -------------
    
    type Ohlc_chg = { 
        T: DateTime; O: float; H: float; L: float; C: float; V: float
        Count: float option
        Wap  : float option }   // <- new

    // If your readOhlc returns a different shape, adapt this mapper.
    // This version assumes a record with fields: T/O/H/L/C/V and optional Count.
    /// Core generic converter with an option for Count
    /// Core converter with an *optional* getter for Count
    let toOhlcChg
        (bars: seq<'a>)
        (getT: 'a -> DateTime)
        (getO: 'a -> float)
        (getH: 'a -> float)
        (getL: 'a -> float)
        (getC: 'a -> float)
        (getV: 'a -> float)
        (getCountOpt: ('a -> float option) option)
        (getWapOpt  : ('a -> float option) option)
        : Ohlc_chg list =
        let getCountFn = defaultArg getCountOpt (fun _ -> None)
        let getWapFn   = defaultArg getWapOpt   (fun _ -> None)
        bars
        |> Seq.map (fun b ->
            { T = getT b; O = getO b; H = getH b; L = getL b; C = getC b; V = getV b;
                Count = getCountFn b;
                Wap   = getWapFn b }
                )
        |> Seq.sortBy (fun x -> x.T)
        |> Seq.toList

    let toOhlcChgNoCountNoWap bars gT gO gH gL gC gV =
        toOhlcChg bars gT gO gH gL gC gV None None

    let toOhlcChgWithCountAndWap bars gT gO gH gL gC gV gCount gWap =
        toOhlcChg bars gT gO gH gL gC gV (Some gCount) (Some gWap)


    // ------------- Safe delta helpers -------------
    let inline private relDelta (prev: float) (curr: float) =
        if abs prev < 1e-8 then None
        else Some ((curr - prev) / prev)

    let inline private safe name prev curr =
        match relDelta prev curr with
        | Some v -> Some (name, v)
        | None   -> None

    // Typical price as a WAP-ish proxy if you need it
    let inline private typ (h:float) (l:float) (c:float) = (h + l + c) / 3.0
    

    let makeChgMap (prev: Ohlc_chg) (curr: Ohlc_chg) : Map<string,float> =
        let wap_prev = prev.Wap |> Option.defaultValue ( (prev.O + prev.H + prev.L + prev.C) / 4.0 )  // simple avg of OHLC
        let wap_curr = curr.Wap |> Option.defaultValue ( (curr.O + curr.H + curr.L + curr.C) / 4.0 )

        // ... rest unchanged ...
        let oc_hl_prev = (prev.O * prev.C) - (prev.H * prev.L)
        let oc_hl_curr = (curr.O * curr.C) - (curr.H * curr.L)
        let hi_lo_prev = prev.H - prev.L
        let hi_lo_curr = curr.H - curr.L
        let oxhxLxC_prev = prev.O * prev.H * prev.L * prev.C
        let oxhxLxC_curr = curr.O * curr.H * curr.L * curr.C

        let vxc_prev = match prev.Count with Some cnt -> prev.V * cnt | None -> nan
        let vxc_curr = match curr.Count with Some cnt -> curr.V * cnt | None -> nan

        [
          safe "chg_WAP"      wap_prev        wap_curr
          safe "chg_CL"       prev.C          curr.C
          safe "chg_OPxCL"    (prev.O*prev.C) (curr.O*curr.C)
          safe "chg_HI_LO"    hi_lo_prev      hi_lo_curr
          safe "chg_OxHxLxC"  oxhxLxC_prev    oxhxLxC_curr
          safe "chg_V"        prev.V          curr.V
          if not (Double.IsNaN vxc_prev || Double.IsNaN vxc_curr) then
            safe "chg_VxCount" vxc_prev vxc_curr else None
          safe "chg_OC_HL"    oc_hl_prev      oc_hl_curr
        ]
        |> List.choose id
        |> Map.ofList

    

    // ------------- Build per-bar change series -------------
    /// For a stream of OHLC bars, produce (timestamp, name, value) triples for each change field.
    let buildChangeTriples (bars: Ohlc_chg list) : (DateTime * string * float) list =
        bars
        |> List.pairwise
        |> List.collect (fun (p,c) ->
            let m = makeChgMap p c
            m |> Map.toList |> List.map (fun (k,v) -> c.T, k, v))


    /// Optional: reshape into separate series per name
    let groupByName (triples: (DateTime * string * float) list) : Map<string, (DateTime * float) list> =
        triples
        |> List.groupBy (fun (_,name,_) -> name)
        |> List.map (fun (name, rows) ->
            name, (rows |> List.map (fun (t,_,v) -> t,v) |> List.sortBy fst))
        |> Map.ofList

    let buildWapSeries (bars: Ohlc_chg list) : (DateTime * float) list =
        bars
        |> List.map (fun b ->
            let v = b.Wap |> Option.defaultValue ( (b.O + b.H + b.L + b.C) / 4.0 )
            b.T, v)
        |> List.sortBy fst

    
