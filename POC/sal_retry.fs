namespace POC

module Retry =
    open System
    open System.Threading
    open System.Threading.Tasks

    // ----------------------------------------------------------------
    // Types
    // ----------------------------------------------------------------
    /// How many retries (max), initial delay, and exponential backoff (e.g., 2.0)
    type RetrySpec =
        { max     : int
          delay   : TimeSpan
          backoff : float }   // e.g., 2.0 ? 100ms, 200ms, 400ms, ...

    // ----------------------------------------------------------------
    // Internal helpers
    // ----------------------------------------------------------------
    let private clampBackoff (b: float) =
        if Double.IsNaN b || Double.IsInfinity b || b <= 0.0 then 1.0 else b

    let private nextDelay (delay: TimeSpan) (backoff: float) (jitter: float option) =
        // jitter ~= 0.1 means +/-10% randomization
        let baseMs = delay.TotalMilliseconds
        let scaled = baseMs * (clampBackoff backoff)
        match jitter with
        | None -> TimeSpan.FromMilliseconds scaled
        | Some j when j > 0.0 ->
            let rnd = System.Random()
            let delta = j * (2.0 * rnd.NextDouble() - 1.0) // [-j, +j]
            let jittered = scaled * (1.0 + delta)
            TimeSpan.FromMilliseconds(Math.Max(0.0, jittered))
        | _ -> TimeSpan.FromMilliseconds scaled

    // ----------------------------------------------------------------
    // Synchronous
    // ----------------------------------------------------------------
    /// Retry a synchronous function returning 'T
    let withRetrySync<'T>
        (spec  : RetrySpec option)
        (f     : unit -> 'T)
        (jitter : float option)
        : 'T =
        match spec with
        | None -> f()
        | Some r ->
            let rec go attempt (delay:TimeSpan) =
                try f() with ex ->
                    if attempt >= r.max then raise ex
                    Thread.Sleep delay
                    let d' = nextDelay delay r.backoff jitter
                    go (attempt + 1) d'
            go 0 r.delay

    /// Like withRetrySync, but returns Result instead of raising
    let tryWithRetrySync<'T>
        (spec  : RetrySpec option)
        (f     : unit -> 'T)
        (jitter : float option)
        : Result<'T, exn> =
        try Ok (withRetrySync spec f jitter)
        with ex -> Error ex

    // ----------------------------------------------------------------
    // Task (async/await on .NET Task)
    // ----------------------------------------------------------------
    /// Retry a Task-returning function. Honors optional CancellationToken for backoff sleeping.
    let withRetryTask<'T>
        (spec  : RetrySpec option)
        (f     : unit -> Task<'T>)
        (ctOpt   : CancellationToken option)   // <-- plain option, no '?'
        (jitter  : float option)               // <-- plain option, no '?'
        : Task<'T> =
        
        match spec with
        | None -> f()
        | Some r ->
            let ct = defaultArg ctOpt CancellationToken.None
            let rec go attempt (delay : TimeSpan) = task {
                try
                    return! f()
                with ex ->
                    if attempt >= r.max then 
                        return! Task.FromException<'T>(ex) //raise ex
                    else
                        do! Task.Delay(delay, ct)
                        let d' = nextDelay delay r.backoff jitter
                        return! go (attempt + 1) d'
            }
            go 0 r.delay

    /// Like withRetryTask, but returns Result instead of raising
    let tryWithRetryTask<'T>
        (spec  : RetrySpec option)
        (f     : unit -> Task<'T>)
        (ct   : CancellationToken option)
        (jitter : float option)
        : Task<Result<'T, exn>> = task {
        try
            let! v = withRetryTask spec f ct jitter
            return Ok v
        with ex ->
            return Error ex
    }

    // ----------------------------------------------------------------
    // F# Async
    // ----------------------------------------------------------------
    /// Retry an Async-returning function.
    let withRetryAsync<'T>
        (spec  : RetrySpec option)
        (f     : unit -> Async<'T>)
        (jitter : float option)
        : Async<'T> =
        match spec with
        | None -> f()
        | Some r ->
            let rec go attempt (delay: TimeSpan) = async {
                try
                    return! f()
                with ex ->
                    if attempt >= r.max then return raise ex
                    do! Async.Sleep (int delay.TotalMilliseconds)
                    let d' = nextDelay delay r.backoff jitter
                    return! go (attempt + 1) d'
            }
            go 0 r.delay

    /// Like withRetryAsync, but returns Result instead of raising
    let tryWithRetryAsync<'T>
        (spec  : RetrySpec option)
        (f     : unit -> Async<'T>)
        (jitter : float option)
        : Async<Result<'T, exn>> = async {
        try
            let! v = withRetryAsync spec f jitter
            return Ok v
        with ex ->
            return Error ex
    }
