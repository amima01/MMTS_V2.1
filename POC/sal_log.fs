namespace POC

module Log =
    open System
    open System.IO
    open Serilog

    let mutable private initialized = false

    let init (appName: string) (logDir: string) =
        if not initialized then
            Directory.CreateDirectory logDir |> ignore
            let logPath = Path.Combine(logDir, appName + "-.log")
            Log.Logger <-
                LoggerConfiguration()
                    .MinimumLevel.Information()
                    .Enrich.FromLogContext()
                    .Enrich.WithProperty("App", appName)
                    .WriteTo.Console(outputTemplate = "[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj}{NewLine}{Exception}")
                    .WriteTo.File(
                        path              = logPath,
                        rollingInterval   = RollingInterval.Day,
                        retainedFileCountLimit = Nullable 14,
                        //shared            = false,
                        buffered          = true,
                        outputTemplate    = "[{Timestamp:yyyy-MM-dd HH:mm:ss.fff} {Level:u3}] {Message:lj}{NewLine}{Exception}"
                    )
                    .CreateLogger()
            initialized <- true

    
    // Convenience wrappers (message-template style)
    let info (message: string) = Log.Information(message)
    let infof fmt = Printf.kprintf (fun s -> Log.Information("{Message}", s)) fmt

    let warn (message: string) = Log.Warning(message)
    let warnf fmt = Printf.kprintf (fun s -> Log.Warning("{Message}", s)) fmt

    let error (message: string) = Log.Error(message)
    let errorf fmt = Printf.kprintf (fun s -> Log.Error("{Message}", s)) fmt

    let debug (message: string) = Log.Debug(message)
    let debugf fmt = Printf.kprintf (fun s -> Log.Debug("{Message}", s)) fmt
    (*
    let info   (tmpl: string) ([<ParamArray>] args: obj[]) = Serilog.Log.Information(tmpl, args)
    let warn   (tmpl: string) ([<ParamArray>] args: obj[]) = Serilog.Log.Warning(tmpl, args)
    let error  (tmpl: string) ([<ParamArray>] args: obj[]) = Serilog.Log.Error(tmpl, args)
    let debug  (tmpl: string) ([<ParamArray>] args: obj[]) = Serilog.Log.Debug(tmpl, args)
    *)
    let close() = Serilog.Log.CloseAndFlush()
