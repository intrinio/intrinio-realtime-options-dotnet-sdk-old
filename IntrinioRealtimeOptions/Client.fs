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
            Symbol = Encoding.ASCII.GetString(bytes, 0, 21)
            Type = enum<QuoteType> (int32 bytes.[21])
            Price = BitConverter.ToDouble(bytes, 22)
            Size = BitConverter.ToUInt32(bytes, 30)
            Timestamp = BitConverter.ToDouble(bytes, 34)
        }

    let heartbeatFn () =
        let ct = ctSource.Token
        Log.Information("Starting heartbeat")
        while not(ct.IsCancellationRequested) && not(isNull(ws)) && isReady do
            try
                Thread.Sleep(20000) //send heartbeat every 20 sec
                Log.Information("Messages received: (data = {0}, text = {1}, queue depth = {2})", dataMsgCount, textMsgCount, data.Count)
                let mutable datum : byte[] = Array.empty<byte>
                if data.TryTake(&datum, 1000, ct)
                then
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


