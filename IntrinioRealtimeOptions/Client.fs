namespace Intrinio

open System
open System.IO
open System.Net
open System.Text
open System.Collections.Concurrent
open System.Collections.Generic
open System.Threading
open WebSocket4Net
open Serilog
open Intrinio.Config

type Client(onQuote : Action<Quote>) =
    let [<Literal>] heartbeatMessage : string = "{\"topic\":\"phoenix\",\"event\":\"heartbeat\",\"payload\":{},\"ref\":null}"
    let [<Literal>] heartbeatResponse : string = "{\"topic\":\"phoenix\",\"ref\":null,\"payload\":{\"status\":\"ok\",\"response\":{}},\"event\":\"phx_reply\"}"
    let [<Literal>] heartbeatErrorResponse : string = "\"status\":\"error\""

    let config = LoadConfig()
    let mutable isReady : bool = false
    let mutable ws : WebSocket = null
    let mutable dataMsgCount : int64 = 0L
    let mutable textMsgCount : int64 = 0L
    let channels : LinkedList<string> = new LinkedList<string>()
    let ctSource : CancellationTokenSource = new CancellationTokenSource()
    let data : BlockingCollection<byte[]> = new BlockingCollection<byte[]>(new ConcurrentQueue<byte[]>())

    let authUrl : string =
        match config.Provider with
        | Provider.OPRA -> "https://realtime-options.intrinio.com/auth?api_key=" + config.ApiKey
        | Provider.OPRA_FIREHOSE -> "https://realtime-options-firehose.intrinio.com/auth?api_key=" + config.ApiKey
        | Provider.MANUAL -> "http://" + config.IPAddress + "/auth?api_key=" + config.ApiKey
        | _ -> failwith "Provider not specified!"

    let parseMessage (bytes: byte[]) : Quote =
        {
            Type = "trade"
            Symbol = Encoding.ASCII.GetString(bytes, 0, 21)
            Price = BitConverter.ToDouble(bytes, 21)
            Size = BitConverter.ToUInt32(bytes, 29)
            Timestamp = BitConverter.ToDouble(bytes, 33)
        }

    let heartbeatFn () =
        let ct = ctSource.Token
        Log.Information("Starting heartbeat")
        while not(ct.IsCancellationRequested) && not(isNull(ws)) && isReady do
            try
                Thread.Sleep(20000) //send heartbeat every 20 sec
                Log.Information("Messages received: (data = {0}, text = {1}, queue depth = {2})", dataMsgCount, textMsgCount, data.Count)
                let datum : byte[] = data.Take(ct)
                let quote : Quote = parseMessage(datum)
                Log.Information("Sample Quote: {0}", quote)
                onQuote.Invoke(quote)
                Log.Debug("Sending heartbeat")
                if not(ct.IsCancellationRequested) && not(isNull(ws)) && isReady
                then ws.Send(heartbeatMessage)
            with :? OperationCanceledException -> ()

    let heartbeat : Thread =
        new Thread(new ThreadStart(heartbeatFn))

    let threadFn () : unit =
        let ct = ctSource.Token
        while not (ct.IsCancellationRequested) && not(isNull(ws)) && isReady do
            try
                let datum : byte[] = data.Take(ct)
                let quote : Quote = parseMessage(datum)
                Log.Debug("Invoking 'onQuote'")
                onQuote.Invoke(quote)
            with :? OperationCanceledException -> ()

    let threads : Thread[] = Array.init config.NumThreads (fun _ -> new Thread(new ThreadStart(threadFn)))

    let onOpen (_ : EventArgs) : unit =
        Log.Information("Websocket connected!")
        isReady <- true
        heartbeat.Start()
        for thread in threads do thread.Start()

    let onClose (_ : EventArgs) : unit =
        Log.Information("Websocket closed!")
        isReady <- false
        ctSource.Cancel()

    let onError (args : SuperSocket.ClientEngine.ErrorEventArgs) : unit =
        Log.Error(args.Exception, "Websocket error!")

    let onDataReceived (args: DataReceivedEventArgs) : unit =
        Log.Debug("Data received")
        Interlocked.Increment(&dataMsgCount) |> ignore
        try
            data.Add(args.Data, ctSource.Token)
        with :? OperationCanceledException -> ()

    let onMessageReceived (args : MessageReceivedEventArgs) : unit =
        Log.Debug("Message received")
        Interlocked.Increment(&textMsgCount) |> ignore
        if args.Message = heartbeatResponse then Log.Debug("Heartbeat response received")
        elif args.Message.Contains(heartbeatErrorResponse) then Log.Error("Heartbeat error received")

    do
        Log.Information("Authorizing...")
        let token : string =
            try
                HttpWebRequest.Create(authUrl).GetResponse() :?> HttpWebResponse
                |> fun response ->
                    match response.StatusCode with
                    | HttpStatusCode.OK ->
                        let stream : Stream = response.GetResponseStream()
                        let reader : StreamReader = new StreamReader(stream, Encoding.UTF8)
                        let _token : string = reader.ReadToEnd()
                        Log.Information("Authorization successful")
                        _token
                    | _ -> raise (AccessViolationException("Authorization Failure: " + response.StatusCode.ToString()))
            with
            | :? WebException | :? IOException as exn ->
                Log.Error("Authorization Failure: {0}. The authorization server is likey offline. Please make sure you're trying to connect during market hours.", exn.Message); Environment.Exit(-1); null
            | :? AccessViolationException as exn -> Log.Error("{0). The authorization key you provided is likely incorrect.", exn.Message); Environment.Exit(-1); null
            | _ as exn -> Log.Error("Unidentified Authorization Failure: {0}", exn.Message); Environment.Exit(-1); null

        let wsUrl =
            match config.Provider with
            | Provider.OPRA -> "wss://realtime-options.intrinio.com/socket/websocket?vsn=1.0.0&token=" + token
            | Provider.OPRA_FIREHOSE -> "wss://realtime-options-firehose.intrinio.com/socket/websocket?vsn=1.0.0&token=" + token
            | Provider.MANUAL -> "ws://" + config.IPAddress + "/socket/websocket?vsn=1.0.0&token=" + token
            | _ -> failwith "Provider not specified!"

        Log.Information("Connecting...")
        ws <- new WebSocket(wsUrl)
        ws.Opened.Add(onOpen)
        ws.Closed.Add(onClose)
        ws.Error.Add(onError)
        ws.DataReceived.Add(onDataReceived)
        ws.MessageReceived.Add(onMessageReceived)
        ws.Open()

    member this.Join() : unit =
        while not(isReady) do Thread.Sleep(1000)
        for symbol in config.Symbols do
            Log.Information("Joining channel: {0}", symbol)
            channels.AddLast(symbol) |> ignore
            let message = "{\"topic\":\"options:" + symbol + "\",\"event\":\"phx_join\",\"payload\":{},\"ref\":null}"
            ws.Send(message)

    member this.Stop() : unit =
        while channels.Count > 0 do
            let channel = channels.First.Value
            Log.Information("Leaving channel: {0}", channel)
            channels.RemoveFirst()
            let message = "{\"topic\":\"options:" + channel + "\",\"event\":\"phx_leave\",\"payload\":{},\"ref\":null}"
            ws.Send(message)
        Thread.Sleep(1000)
        isReady <- false
        ctSource.Cancel()
        ws.Close()
        heartbeat.Join()
        for thread in threads do thread.Join()
        Log.Information("Stopped")

    static member Log(messageTemplate:string, [<ParamArray>] propertyValues:obj[]) = Log.Information(messageTemplate, propertyValues)

module internal Client =
    let parseMessage (bytes: byte[]) : Quote =
        {
            Type = "trade"
            Symbol = Encoding.ASCII.GetString(bytes, 0, 21)
            Price = BitConverter.ToDouble(bytes, 21)
            Size = BitConverter.ToUInt32(bytes, 29)
            Timestamp = BitConverter.ToDouble(bytes, 33)
        }
        
    let authUrl (config : Config) i =
        match config.Provider with
        | Provider.OPRA -> "https://realtime-options.intrinio.com/auth?api_key=" + config.ApiKey
        | Provider.OPRA_FIREHOSE -> "https://realtime-options-firehose.intrinio.com/auth?api_key=" + config.ApiKey
        | Provider.MANUAL -> "http://" + config.IPAddress + ":800" + (i.ToString()) + "/auth?api_key=" + config.ApiKey
        | _ -> failwith "Provider not specified!"
        
   
    let getToken (config : Config) i =
            try
                HttpWebRequest.Create(authUrl config i).GetResponse() :?> HttpWebResponse
                |> fun response ->
                    match response.StatusCode with
                    | HttpStatusCode.OK ->
                        let stream : Stream = response.GetResponseStream()
                        let reader : StreamReader = new StreamReader(stream, Encoding.UTF8)
                        let _token : string = reader.ReadToEnd()
                        Log.Information("Authorization successful")
                        _token
                    | _ -> raise (AccessViolationException("Authorization Failure: " + response.StatusCode.ToString()))
            with
            | :? WebException | :? IOException as exn ->
                Log.Error("Authorization Failure: {0}. The authorization server is likey offline. Please make sure you're trying to connect during market hours.", exn.Message); Environment.Exit(-1); null
            | :? AccessViolationException as exn -> Log.Error("{0). The authorization key you provided is likely incorrect.", exn.Message); Environment.Exit(-1); null
            | _ as exn -> Log.Error("Unidentified Authorization Failure: {0}", exn.Message); Environment.Exit(-1); null
     
    let wsUrl (config : Config) token i =
        match config.Provider with
        | Provider.OPRA -> "wss://realtime-options.intrinio.com/socket/websocket?vsn=1.0.0&token=" + token
        | Provider.OPRA_FIREHOSE -> "wss://realtime-options-firehose.intrinio.com/socket/websocket?vsn=1.0.0&token=" + token
        | Provider.MANUAL -> "ws://" + config.IPAddress + ":800" + (i.ToString()) + "/socket/websocket?vsn=1.0.0&token=" + token
        | _ -> failwith "Provider not specified!"
        
        
open Client    
type MultiClient(onQuote : Action<Quote>) =
     
    let [<Literal>] heartbeatMessage : string = "{\"topic\":\"phoenix\",\"event\":\"heartbeat\",\"payload\":{},\"ref\":null}"
    let [<Literal>] heartbeatResponse : string = "{\"topic\":\"phoenix\",\"ref\":null,\"payload\":{\"status\":\"ok\",\"response\":{}},\"event\":\"phx_reply\"}"
    let [<Literal>] heartbeatErrorResponse : string = "\"status\":\"error\""

    let config = LoadConfig()
    let mutable isReady : bool = false
    let mutable wss : WebSocket[] = null
    let mutable dataMsgCount : int64 = 0L
    let mutable textMsgCount : int64 = 0L
    let channels : LinkedList<string> = new LinkedList<string>()
    let ctSource : CancellationTokenSource = new CancellationTokenSource()
    let data : BlockingCollection<byte[]>[] = Array.init config.NumPorts (fun _ -> new BlockingCollection<byte[]>(new ConcurrentQueue<byte[]>()))





    let heartbeatFn porti () =
        let ct = ctSource.Token
        Log.Information("Starting heartbeat")
        while not(ct.IsCancellationRequested) && not(isNull(wss)) && isReady do
            try
                Thread.Sleep(20000) //send heartbeat every 20 sec
                Log.Information("Messages received: (data = {0}, text = {1}, queue depth = {2})", dataMsgCount, textMsgCount, data.[porti].Count)
//                let datum : byte[] = data.Take(ct)
//                let quote : Quote = parseMessage(datum)
//                Log.Information("Sample Quote: {0}", quote)
//                onQuote.Invoke(quote)
                Log.Debug("Sending heartbeat")
                if not(ct.IsCancellationRequested) && not(isNull(wss)) && isReady
                then
                    wss.[porti].Send(heartbeatMessage)
            with :? OperationCanceledException -> ()

    let heartbeat : Thread[] = Array.init config.NumPorts (fun porti -> new Thread(new ThreadStart(heartbeatFn porti)))
        

    let threadFn porti threadi () : unit =
        let ct = ctSource.Token
        while not (ct.IsCancellationRequested) && not(isNull(wss)) && isReady do
            try
                let datum : byte[] = data.[porti].Take(ct)
                let quote : Quote = parseMessage(datum)
                Log.Debug("Invoking 'onQuote'")
                onQuote.Invoke(quote)
            with :? OperationCanceledException -> ()

    let threads : Thread[,] = Array2D.init config.NumPorts config.NumThreads (fun porti threadi -> new Thread(new ThreadStart(threadFn porti threadi)))

    let onOpen porti (_ : EventArgs) : unit =
        Log.Information("Websocket connected!")
        isReady <- true
        heartbeat.[porti].Start()
        threads |> Array2D.iteri (fun pi ti thread ->
            if pi = porti then thread.Start()
            )


    let onClose (_ : EventArgs) : unit =
        Log.Information("Websocket closed!")
        isReady <- false
        ctSource.Cancel()

    let onError (args : SuperSocket.ClientEngine.ErrorEventArgs) : unit =
        Log.Error(args.Exception, "Websocket error!")

    let onDataReceived porti (args: DataReceivedEventArgs) : unit =
        Log.Debug("Data received")
        Interlocked.Increment(&dataMsgCount) |> ignore
        try
            data.[porti].Add(args.Data, ctSource.Token)
        with :? OperationCanceledException -> ()

    let onMessageReceived (args : MessageReceivedEventArgs) : unit =
        Log.Debug("Message received")
        Interlocked.Increment(&textMsgCount) |> ignore
        if args.Message = heartbeatResponse then Log.Debug("Heartbeat response received")
        elif args.Message.Contains(heartbeatErrorResponse) then Log.Error("Heartbeat error received")

    do
        Log.Information("Authorizing...")
        let token = getToken config 0

        wss <- Array.init config.NumPorts (fun i ->
            Log.Information("Connecting... {0}",i)
            let ws = new WebSocket(wsUrl config token i)
            ws.Opened.Add(onOpen i)
            ws.Closed.Add(onClose)
            ws.Error.Add(onError)
            ws.DataReceived.Add(onDataReceived i)
            ws.MessageReceived.Add(onMessageReceived)
            ws.Open()
            ws
            )

        

    member this.Join() : unit =
        while not(isReady) do Thread.Sleep(1000)
        for symbol in config.Symbols do
            Log.Information("Joining channel: {0}", symbol)
            channels.AddLast(symbol) |> ignore
            let message = "{\"topic\":\"options:" + symbol + "\",\"event\":\"phx_join\",\"payload\":{},\"ref\":null}"
            for ws in wss do ws.Send(message)

    member this.Stop() : unit =
        while channels.Count > 0 do
            let channel = channels.First.Value
            Log.Information("Leaving channel: {0}", channel)
            channels.RemoveFirst()
            let message = "{\"topic\":\"options:" + channel + "\",\"event\":\"phx_leave\",\"payload\":{},\"ref\":null}"
            for ws in wss do ws.Send(message)
        Thread.Sleep(1000)
        isReady <- false
        ctSource.Cancel()
        for ws in wss do ws.Close()
        for h in heartbeat do h.Join()
        threads |> Array2D.iter (fun thread -> thread.Join())
        //for thread in threads do thread.Join()
        Log.Information("Stopped")

    static member Log(messageTemplate:string, [<ParamArray>] propertyValues:obj[]) = Log.Information(messageTemplate, propertyValues)