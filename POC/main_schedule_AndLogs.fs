namespace POC
module  main_schedule_AndLogs =
    open System
    open System.Threading
    open System.Threading.Tasks
    open Quartz
    open Quartz.Impl
    open POC.Log
    open POC.Schedule
    
    let _run (s: string) =
        Log.init "POC" "logs"
    
        let jobs =
            [ ScheduledJob.ofTask "heartbeat" (Interval (TimeSpan.FromSeconds 10.0)) (fun () -> task {
                  Log.infof "Heartbeat at %O" DateTime.UtcNow
                  return ()
              })
              ScheduledJob.ofTask "dailyRun" (Cron "0 0 9 * * ?") (fun () -> task {
                  Log.info "Running daily batch"
                  return ()
              }) ]
        // start scheduler and keep a handle so we can shut it down
        let factory = StdSchedulerFactory()
        let sched = factory.GetScheduler().GetAwaiter().GetResult()
        do sched.Start().GetAwaiter().GetResult()
        // register jobs via our helper
        jobs |> List.iter (fun j ->
            JobRegistry.addTask j.Id j.Action
            let qj = JobBuilder.Create<ActionJob>().WithIdentity(j.Id).UsingJobData("jobId", j.Id).Build()
            let qt =  // same createTrigger logic as in Schedule.fs
                match j.Schedule with
                | Interval ts -> TriggerBuilder.Create().StartNow()
                                   .WithSimpleSchedule(fun x -> x.WithInterval(ts).RepeatForever() |> ignore).Build()
                | Cron expr   -> TriggerBuilder.Create().WithCronSchedule(expr).Build()
                | OnceAt dt   -> TriggerBuilder.Create().StartAt(dt).Build()
            sched.ScheduleJob(qj, qt).GetAwaiter().GetResult() |> ignore)

        // graceful Ctrl+C
        use quit = new ManualResetEventSlim(false)
        Console.CancelKeyPress.Add(fun e ->
            e.Cancel <- true
            quit.Set()
        )

        Console.WriteLine("Scheduler started. Press Ctrl+C to exit.")
        quit.Wait()

        try
            sched.Shutdown(waitForJobsToComplete = true).GetAwaiter().GetResult()
        finally
            Log.close()
        
        0



