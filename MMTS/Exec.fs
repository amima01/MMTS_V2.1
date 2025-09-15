namespace MMTS.ML

open System
open System.Collections.Generic
open System.Data
open System.Reflection
open System.Text
open Microsoft.Data.SqlClient
open Types

module Exec =

    // ---------- Parameter overrides ----------
    let applyParamOverrides (wf: WorkflowSpec) (ovr: Map<string,string>) : WorkflowSpec =
        let dict' = Dictionary<string,string>(wf.Params, StringComparer.OrdinalIgnoreCase) :> IDictionary<string,string>
        for KeyValue(k,v) in ovr do
            (dict' :?> Dictionary<string,string>).[k] <- v
        { wf with Params = dict' }

    // ---------- Basic validation ----------
    let validate (wf: WorkflowSpec) : string list =
        let errs = ResizeArray<string>()
        if String.IsNullOrWhiteSpace wf.Name then errs.Add "name is required"
        if wf.Steps.IsEmpty then errs.Add "steps is empty"
        for s in wf.Steps do
            if String.IsNullOrWhiteSpace s.Id then errs.Add "step.id is required"
            match s.Kind with
            | StepKind.Materialize ->
                // minimal checks for materialize
                if s.Inp.from.IsNone then errs.Add $"materialize step '{s.Id}' missing in.from"
            | _ -> ()
        errs |> List.ofSeq

    // ---------- Interpolation ----------
    let private tryParam (wf: WorkflowSpec) (key: string) =
        match wf.Params.TryGetValue key with
        | true, v -> Some v
        | _ -> None

    let private replaceAll (s:string, oldValue:string, newValue:string) =
        if String.IsNullOrEmpty s then s else s.Replace(oldValue, newValue)

    /// ${param:key} | ${ENV:NAME|default} | ${NOW:format}
    let private resolveString (wf: WorkflowSpec) (s: string) =
        if String.IsNullOrEmpty s then s else
        let mutable txt = s

        // param
        let rec subParam () =
            let i = txt.IndexOf("${param:")
            if i < 0 then () else
            let j = txt.IndexOf("}", i+8)
            if j < 0 then () else
            let key = txt.Substring(i+8, j-(i+8))
            let repl = defaultArg (tryParam wf key) ""
            txt <- txt.Substring(0,i) + repl + txt.Substring(j+1)
            subParam ()
        subParam ()

        // ENV
        let rec subEnv () =
            let i = txt.IndexOf("${ENV:")
            if i < 0 then () else
            let j = txt.IndexOf("}", i+6)
            if j < 0 then () else
            let body = txt.Substring(i+6, j-(i+6)) // NAME|default?
            let name, def =
                match body.Split('|') with
                | [|nm; df|] -> nm, Some df
                | [|nm|]     -> nm, None
                | _          -> body, None
            let valOpt = Environment.GetEnvironmentVariable(name)
            let repl =
                if String.IsNullOrEmpty valOpt then defaultArg def "" else valOpt
            txt <- txt.Substring(0,i) + repl + txt.Substring(j+1)
            subEnv ()
        subEnv ()

        // NOW
        let rec subNow () =
            let i = txt.IndexOf("${NOW:")
            if i < 0 then () else
            let j = txt.IndexOf("}", i+6)
            if j < 0 then () else
            let fmt = txt.Substring(i+6, j-(i+6))
            let repl =
                try DateTime.Now.ToString(fmt)
                with _ -> DateTime.Now.ToString("yyyy-MM-dd")
            txt <- txt.Substring(0,i) + repl + txt.Substring(j+1)
            subNow ()
        subNow ()

        txt

    let private resolveArgs (wf: WorkflowSpec) (args: IDictionary<string,obj>) =
        let d = Dictionary<string,obj>(StringComparer.OrdinalIgnoreCase) :> IDictionary<string,obj>
        for kv in args do
            let v =
                match kv.Value with
                | :? string as s -> box (resolveString wf s)
                | :? ResizeArray<string> as arr ->
                    // interpolate each element
                    arr |> Seq.map (resolveString wf) |> ResizeArray |> box
                | other -> other
            d.[kv.Key] <- v
        d

    // ---------- Hook invocation ----------
    let private invokeHook (hook: HookSpec) (args: IDictionary<string,string>) : obj =
        let asm =
            // Try to find loaded assembly by root namespace of the module
            let parts = hook.Module.Split('.')
            let asmName = parts |> Array.tryHead |> Option.defaultValue hook.Module
            AppDomain.CurrentDomain.GetAssemblies()
            |> Array.tryFind (fun a -> a.GetName().Name = asmName)
            |> Option.defaultWith (fun _ ->
                // Fallback: search by full type name owner
                let tAsm =
                    AppDomain.CurrentDomain.GetAssemblies()
                    |> Array.tryPick (fun a -> a.GetType(hook.Module) |> Option.ofObj |> Option.map (fun _ -> a))
                defaultArg tAsm (failwithf "Assembly not found for %s" hook.Module)
            )

        let t = asm.GetType(hook.Module, throwOnError = true, ignoreCase = false)
        let mi = t.GetMethod(hook.Func, BindingFlags.Public ||| BindingFlags.Static)
        if isNull mi then failwithf "Function %s not found on %s" hook.Func hook.Module
        // Expect signature: Run (IDictionary<string,string>) : obj
        mi.Invoke(null, [| args |])

    // ---------- Emit (dry plan) ----------
    let emitDryRun (wf: WorkflowSpec) : string =
        let sb = StringBuilder()
        sb.AppendLine($"workflow: {wf.Name} (v{wf.Version})") |> ignore
        sb.AppendLine("params:") |> ignore
        if wf.Params.Count = 0 then sb.AppendLine("  (none)") |> ignore
        else for kv in wf.Params do sb.AppendLine($"  {kv.Key} = {kv.Value}") |> ignore
        sb.AppendLine("steps:") |> ignore
        for s in wf.Steps do
            let kindStr = match s.Kind with | StepKind.Materialize -> "materialize" | _ -> "normal"
            sb.AppendLine($"  - id: {s.Id}  kind: {kindStr}  in:{s.Inp.io} out:{s.Out.io}") |> ignore
            if s.Args.Count > 0 then
                let r = resolveArgs wf s.Args
                for kv in r do
                    match kv.Value with
                   
                    | :? ResizeArray<string> as arr ->
                        let arrStr = String.Join(", ", arr)
                        sb.AppendLine($"      arg {kv.Key} = [{arrStr}]") |> ignore

                    | _ ->
                        sb.AppendLine($"      arg {kv.Key} = {kv.Value}") |> ignore
        sb.ToString()

    // ---------- SQL helpers for materialize ----------
    let private inferSqlType (v: obj) =
        match v with
        | :? string -> "NVARCHAR(4000)"
        | :? DateTime -> "DATETIME2"
        | :? int | :? int64 | :? int16 -> "BIGINT"
        | :? single | :? float | :? double | :? decimal -> "FLOAT"
        | :? bool -> "BIT"
        | null -> "NVARCHAR(4000)"
        | _ -> "NVARCHAR(4000)"

    let private toDataTable (rows: seq<IDictionary<string,obj>>) =
        let dt = new Data.DataTable("mmts_tmp")
        // establish columns from first row
        let first =
            rows |> Seq.tryHead
            |> Option.defaultWith (fun _ -> failwith "materialize: no rows to write")
        for kv in first do
            // use object type for flexibility; SQL types resolved later
            dt.Columns.Add(kv.Key, typeof<obj>) |> ignore
        for r in rows do
            let values = [| for kv in first -> if r.ContainsKey kv.Key then r.[kv.Key] else null |]
            dt.Rows.Add(values) |> ignore
        dt

    let private execNonQuery (conn: SqlConnection) (sql: string) =
        if String.IsNullOrWhiteSpace sql then 0 else
        use cmd = new SqlCommand(sql, conn)
        cmd.CommandTimeout <- 120
        cmd.ExecuteNonQuery()

    let private bulkUpsert
        (connStr: string)
        (schema: string)
        (target: string)
        (keys: string list)
        (rows: seq<IDictionary<string,obj>>)
        (preSql: string option)
        (postSql: string option)
        =
        use conn = new SqlConnection(connStr)
        conn.Open()

        preSql |> Option.iter (fun s -> execNonQuery conn s |> ignore)

        let dt = toDataTable rows
        let gid  = Guid.NewGuid().ToString("N")
        let temp = $"#mmts_tmp_{gid}"
        //let temp = $"#mmts_tmp_{Guid.NewGuid().ToString("N")}"

        // create temp table with loose types
        (*
        let cols = [ for c in dt.Columns do 
            let dc = c :?> Data.DataColumn in dc.ColumnName, "NVARCHAR(4000)" ]
       
        let cols = [
            for c : Data.DataColumn in dt.Columns do
                bc.ColumnMappings.Add(c.ColumnName, c.ColumnName) |> ignore
            ]
         *)
        let cols =
            dt.Columns
            |> Seq.cast<Data.DataColumn>
            |> Seq.map (fun c -> c.ColumnName, c.ColumnName)
            |> Seq.toList

        //let createCols = cols |> List.map (fun (n,t) -> $"[{n}] {t} NULL") |> String.Join(", ")
        let createCols =
            cols
            |> List.map (fun (n,t) -> $"[{n}] {t} NULL")
            |> String.concat ", "

        let createTmp = $"CREATE TABLE {temp} ({createCols});"
        execNonQuery conn createTmp |> ignore

        // bulk copy as NVARCHAR (we're sending as obj→ToString when needed)
        use bc = new SqlBulkCopy(conn)
        bc.DestinationTableName <- temp
     
        for c : Data.DataColumn in dt.Columns do
            bc.ColumnMappings.Add(c.ColumnName, c.ColumnName) |> ignore

        bc.WriteToServer(dt)

        // build MERGE
        let tgt = $"[{schema}].[{target}]"
        let allCols = dt.Columns |> Seq.cast<Data.DataColumn> |> Seq.map (fun c -> c.ColumnName) |> Seq.toList
        let onCond =
            keys
            |> List.map (fun k -> $"T.[{k}] = S.[{k}]")
            |> String.concat " AND "

        let upCols =
            allCols |> List.filter (fun c -> not (keys |> List.exists ((=) c)))
        let setClause =
            if upCols.IsEmpty then ""
            else
                upCols |> List.map (fun c -> $"T.[{c}] = S.[{c}]") |> String.concat ", " |> sprintf "WHEN MATCHED THEN UPDATE SET %s"

        let insCols = allCols |> List.map (fun c -> $"[{c}]") |> String.concat ", "
        let insVals = allCols |> List.map (fun c -> $"S.[{c}]") |> String.concat ", "

        let mergeSql = $"""
MERGE {tgt} AS T
USING {temp} AS S
ON {onCond}
{setClause}
WHEN NOT MATCHED BY TARGET THEN
    INSERT ({insCols}) VALUES ({insVals});
"""

        execNonQuery conn mergeSql |> ignore

        // drop temp
        execNonQuery conn $"DROP TABLE {temp};" |> ignore

        postSql |> Option.iter (fun s -> execNonQuery conn s |> ignore)

        ()

    // ---------- Runner ----------
    type ExecContext() =
        let store = Dictionary<string, obj>(StringComparer.OrdinalIgnoreCase)
        member _.TryGet (k:string) = match store.TryGetValue k with | true, v -> Some v | _ -> None
        member _.Set (k:string, v:obj) = store.[k] <- v
        member _.Count with get() = store.Count

    let private coerceRows (payload: obj) : seq<IDictionary<string,obj>> =
        match payload with
        | null -> Seq.empty
        | :? System.Collections.IEnumerable as en ->
            en
            |> Seq.cast<obj>
            |> Seq.choose (fun o ->
                match o with
                | :? IDictionary<string,obj> as d -> Some d
                | _ -> None)
        | _ -> Seq.empty

    let run (wf: WorkflowSpec) : Result<string,string> =
        try
            let ctx = ExecContext()

            for step in wf.Steps do
                match step.Kind with
                | StepKind.Normal ->
                    match wf.Hooks.TryGetValue step.Id with
                    | true, hook ->
                        let resolved = resolveArgs wf step.Args
                        let argStr = Dictionary<string,string>(StringComparer.OrdinalIgnoreCase) :> IDictionary<string,string>
                        for kv in resolved do
                            match kv.Value with
                            | :? string as s -> argStr.[kv.Key] <- s
                            | :? ResizeArray<string> as arr -> argStr.[kv.Key] <- String.Join(",", arr)
                            | v -> argStr.[kv.Key] <- v.ToString()
                        let rv = invokeHook hook argStr
                        ctx.Set(step.Id, rv)
                    | _ ->
                        // pass-through from upstream, if any
                        match step.Inp.from with
                        | Some src ->
                            match ctx.TryGet src with
                            | Some v -> ctx.Set(step.Id, v)
                            | None -> ctx.Set(step.Id, null)
                        | None -> ctx.Set(step.Id, null)

                | StepKind.Materialize ->
                    let fromId =
                        match step.Inp.from with
                        | Some f -> f
                        | None -> failwithf "materialize step '%s' missing 'in.from'" step.Id

                    let payload =
                        match ctx.TryGet fromId with
                        | Some v -> v
                        | None -> null

                    let rows = coerceRows payload

                    // Resolve args
                    let args = resolveArgs wf step.Args
                    let getStr name =
                        match args.TryGetValue name with
                        | true, (:? string as s) -> Some s
                        | true, v when not (isNull v) -> Some (v.ToString())
                        | _ -> None

                    let connStr = getStr "connStr" |> Option.defaultWith (fun _ -> failwith "materialize: connStr missing")
                    let schema  = getStr "db.schema" |> Option.orElse (getStr "schema") |> Option.defaultValue "dbo"
                    let target  = getStr "db.target" |> Option.orElse (getStr "target_table") |> Option.defaultValue "(unknown)"
                    // keys can be list or comma
                    let keys =
                        match args.TryGetValue "keys" with
                        | true, (:? ResizeArray<string> as arr) -> arr |> Seq.toList
                        | true, (:? string as s) when s.Contains(",") -> s.Split(',', StringSplitOptions.RemoveEmptyEntries) |> Array.map (fun x -> x.Trim()) |> Array.toList
                        | true, (:? string as s) when s <> "" -> [s]
                        | _ -> []

                    let preSql =
                        getStr "pre_sql" |> Option.filter (fun s -> not (String.IsNullOrWhiteSpace s))
                    let postSql =
                        getStr "post_sql" |> Option.filter (fun s -> not (String.IsNullOrWhiteSpace s))

                    let rowsCount = rows |> Seq.length
                    if rowsCount = 0 then
                        printfn "Materialize: step=%s rows=0 target=%s.%s (SKIPPED)" step.Id schema target
                    else
                        bulkUpsert connStr schema target keys rows preSql postSql
                        printfn "Materialize: step=%s rows=%d target=%s.%s (UPSERTED)" step.Id rowsCount schema target

            Ok (sprintf "Executed %d step(s). Artifacts=%d" wf.Steps.Length (ctx.Count))
        with ex ->
            Error ex.Message

    /// Optional: run up to (and including) a given stepId
    let runStep (wf: WorkflowSpec) (stepId: string) : Result<string,string> =
        let idx = wf.Steps |> List.tryFindIndex (fun s -> s.Id = stepId)
        match idx with
        | None -> Error (sprintf "step '%s' not found" stepId)
        | Some i ->
            let sub = { wf with Steps = wf.Steps |> List.take (i+1) }
            run sub
