namespace POC
module Program =
    open System
    open System.Threading
    open System.Threading.Tasks
    open Quartz
    open Quartz.Impl
    open POC.Log
    open POC.Schedule
    [<EntryPoint>]
    let main _ =
        main_schedule_AndLogs._run ("run scheduler and logging...") |> ignore
        
        
        
        0
