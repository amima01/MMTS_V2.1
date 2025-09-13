namespace POC
// ---- Quartz scheduling (cron) ----
namespace POC

module Schedule =
    open System
    open System.Threading.Tasks
    open Quartz
    open Quartz.Impl

    // ------------------------------------------------------
    // Types
    // ------------------------------------------------------
    type ScheduleKind =
        | Cron of string        // e.g. "0 0/5 * * * ?" ? every 5 minutes
        | Interval of TimeSpan  // fixed interval
        | OnceAt of DateTime    // one-time execution

    /// Actions must be Task-returning to satisfy Quartz IJob
    type ScheduledJob =
        { Id       : string
          Schedule : ScheduleKind
          Action   : unit -> Task }

    module ScheduledJob =
        /// Helper for sync actions
        let ofSync (id: string) (schedule: ScheduleKind) (f: unit -> unit) : ScheduledJob =
            { Id = id
              Schedule = schedule
              Action = fun () -> f(); Task.CompletedTask }

        /// Helper for async/Task actions
        let ofTask (id: string) (schedule: ScheduleKind) (f: unit -> Task) : ScheduledJob =
            { Id = id
              Schedule = schedule
              Action = f }
    // ------------------------------------------------------
    // Quartz job wrapper + registry
    // ------------------------------------------------------
    type ActionJob() =
        interface IJob with
            // Quartz 3: must return Task
            member _.Execute(ctx: IJobExecutionContext) : Task =
                task {
                    let jobId = ctx.JobDetail.JobDataMap.["jobId"] :?> string
                    match JobRegistry.tryFind jobId with
                    | Some action -> do! action()                    
                    | None ->  POC.Log.warnf "No action registered for jobId={%s}" jobId //"Heartbeat at %O" DateTime.UtcNow
                    ////| None -> printfn "No action registered for jobId=%s" jobId
                }

    and JobRegistry private () =
        static let reg = System.Collections.Concurrent.ConcurrentDictionary<string, (unit -> Task)>()
        static member addTask (id: string) (f: unit -> Task) = reg[id] <- f
        static member addSync (id: string) (f: unit -> unit) =
            reg[id] <- (fun () -> f(); Task.CompletedTask)
        static member tryFind (id: string) : (unit -> Task) option =            
            match reg.TryGetValue id with
            | true, f -> Some f
            | _ -> None
    // ------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------
    let private createJob (job: ScheduledJob) =
        JobBuilder
            .Create<ActionJob>()
            .WithIdentity(job.Id)
            .UsingJobData("jobId", job.Id)
            .Build()

    let private createTrigger (job: ScheduledJob) =
        match job.Schedule with
        | Cron expr ->
            TriggerBuilder
                .Create()
                .WithIdentity(job.Id + "-trigger")
                .WithCronSchedule(expr)
                .Build()
        | Interval ts ->
            TriggerBuilder
                .Create()
                .WithIdentity(job.Id + "-trigger")
                .StartNow()
                .WithSimpleSchedule(fun x ->
                    x.WithInterval(ts).RepeatForever() |> ignore)
                .Build()
        | OnceAt dt ->
            TriggerBuilder
                .Create()
                .WithIdentity(job.Id + "-trigger")
                .StartAt(dt)
                .Build()

    

    // ------------------------------------------------------
    // Runner
    // ------------------------------------------------------
    let run (jobs: ScheduledJob list) =
        async {
            let factory = StdSchedulerFactory()
            let! sched = factory.GetScheduler() |> Async.AwaitTask
            do! sched.Start() |> Async.AwaitTask

            for job in jobs do
                JobRegistry.addTask job.Id job.Action
                let j = createJob job
                let t = createTrigger job
                do! sched.ScheduleJob(j, t) |> Async.AwaitTask |> Async.Ignore
        }
        |> Async.Start
