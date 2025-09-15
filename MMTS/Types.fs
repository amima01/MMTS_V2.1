namespace MMTS.ML
// -----------------------------
// Types (AST)
// -----------------------------
module Types =
    type Params =
        { envName   : string
          dbName    : string
          connStr   : string
          hookNs    : string
          impl      : string
          batchId   : string
          emitMode  : string }

    type DataSource = { kind: string; conn: string }
    type TableRef   = { ds: string; name: string }

    type Hook =
        { moduleName: string
          func      : string
          kind      : string } // "rows"

    type StepIO = { rows: string option } // minimal I/O for now

    // ---- execution kinds and options ----
    type RetrySpec =
        { max     : int
          delay   : System.TimeSpan
          backoff : float }     // e.g., 2.0 means exponential backoff

    type IoKind = Nonx | Rows | Json

    type DotNetUse =
        { typeName : string
          method   : string }  // defaults to "Run" if omitted

    type ExeUse =
        { path    : string
          args    : string list
          stdin   : IoKind
          stdout  : IoKind
          timeout : System.TimeSpan
          retry   : RetrySpec option }

    type Use =
        | HookKey of string      // back-compat: key into hooks map
        | DotNet  of DotNetUse   // in-process: static method
        | Exe     of ExeUse      // out-of-process: spawn exe

    // ---- foreach fan-out inside a step ----
    // Supports either:
    //   - over: "rows:step:<id>.rows" | "args:<key>" | "range:<start>..<stop>" | "file:<path>" | "env:<VAR>" | "json:<[...array…]>"
    //   - items: [ "A", "B", "C" ]          (inline list)
    type ForeachSpec =
        { over        : string option
          items       : string list option
          parallelism : int option } // max concurrent items; None/<=1 = sequential

    // A single executable step
    type Step =
        { id       : string
          uses     : Use
          ``in``   : StepIO option
          args     : Map<string,string>
          foreach  : ForeachSpec option
          out      : StepIO option }

    // ---- parallel groups with DOP ----
    type ParallelGroup =
        { steps       : Step list
          parallelism : int option }  // max concurrent child steps; None/<=1 = sequential; 0/unset -> unbounded

    type StepNode =
        | Single   of Step
        | Parallel of ParallelGroup

    type Validation =
        { id  : string
          on  : string
          rule: string }

    type Materialize =
        { from    : string
          ``to``  : string
          mode    : string
          key     : string list
          preSql  : string option
          postSql : string option }

    // ---- cron schedule at workflow level ----
    type ScheduleSpec = { cron: string }

    type Spec =
        { workflow    : string
          version     : string
          description : string
          parameters  : Params
          datasources : Map<string, DataSource>
          tables      : Map<string, TableRef>
          hooks       : Map<string, Hook>
          steps       : StepNode list
          validations : Validation list
          materialize : Materialize list
          schedule    : ScheduleSpec option }

    // Data rows flowing through the pipeline
    type Row  = Map<string,obj>
    type Rows = Row list

    
    
    open System
    open System.Collections.Generic
    
        
    type StepIn =
        { from: string option
          io: IoKind }
    
    type StepOut =
        { io: IoKind }
    
    type HookSpec =
        { Module: string
          Func:   string
          Kind:   string } // "rows" | "json" | "none"
    
    type StepKind =
        | Normal
        | Materialize
    
    type StepSpec =
        { Id:   string
          Kind: StepKind
          Inp:  StepIn
          Out:  StepOut
          Args: IDictionary<string,obj> }
    
    type WorkflowSpec =
        { Name:    string
          Version: string
          Params:  IDictionary<string,string>
          Hooks:   IDictionary<string,HookSpec>
          Steps:   StepSpec list }
    