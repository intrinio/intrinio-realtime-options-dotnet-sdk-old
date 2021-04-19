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

type IDataListener =
    abstract Activate : BlockingCollection<byte[]>[] -> unit

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
        | Provider.OPRA_FIREHOSE -> "wss://realtime-options-firehose.intrinio.com:800" + (i.ToString()) + "/socket/websocket?vsn=1.0.0&token=" + token
        | Provider.MANUAL -> "ws://" + config.IPAddress + ":800" + (i.ToString()) + "/socket/websocket?vsn=1.0.0&token=" + token
        | _ -> failwith "Provider not specified!"
        
        
open Client    
type Client(onQuote : Action<Quote>) =
     
    let [<Literal>] heartbeatMessage : string = "{\"topic\":\"phoenix\",\"event\":\"heartbeat\",\"payload\":{},\"ref\":null}"
    let [<Literal>] heartbeatResponse : string = "{\"topic\":\"phoenix\",\"ref\":null,\"payload\":{\"status\":\"ok\",\"response\":{}},\"event\":\"phx_reply\"}"
    let [<Literal>] heartbeatErrorResponse : string = "\"status\":\"error\""

    let config = LoadConfig()
    let mutable isReady : bool[] = Array.create config.NumPorts false 
    let mutable wss : WebSocket[] = null
    let mutable dataMsgCount : int64 = 0L
    let mutable textMsgCount : int64 = 0L
    let channels : LinkedList<string> = new LinkedList<string>()
    let ctSource : CancellationTokenSource = new CancellationTokenSource()
    let data : BlockingCollection<byte[]>[] = Array.init config.NumPorts (fun _ -> new BlockingCollection<byte[]>(new ConcurrentQueue<byte[]>()))





    let heartbeatFn portIndex () =
        let ct = ctSource.Token
        Log.Information("Starting heartbeat")
        while not(ct.IsCancellationRequested) && not(isNull(wss)) do
            try
                if isReady.[portIndex] then 
                    Thread.Sleep(10000) //send heartbeat every 20 sec
                    Log.Information("Messages received: (data = {0}, text = {1}, queue depth = {2})", dataMsgCount, textMsgCount, data.[portIndex].Count)
                    Log.Debug("Sending heartbeat")
                    if not(ct.IsCancellationRequested) && not(isNull(wss)) && isReady.[portIndex]
                    then
                        wss.[portIndex].Send(heartbeatMessage)
                else if wss.[portIndex].State <> WebSocketState.Open then
                   Log.Information("Reconnecting")
                   wss.[portIndex].Open()
            with :? OperationCanceledException -> ()

    let heartbeat : Thread[] = Array.init config.NumPorts (fun portIndex -> new Thread(new ThreadStart(heartbeatFn portIndex)))
        

    

    let listener : IDataListener =
        let threadFn portIndex threadIndex (data : BlockingCollection<byte[]>[]) () : unit =
            let ct = ctSource.Token
            while not (ct.IsCancellationRequested) && isReady.[portIndex] do
                try
                    let datum : byte[] = data.[portIndex].Take(ct)
                    let quote : Quote = parseMessage(datum)
                    Log.Debug("Invoking 'onQuote'")
                    onQuote.Invoke(quote)
                with :? OperationCanceledException -> ()
        { new IDataListener with
                                       
            member this.Activate(data) =
                Array2D.init config.NumPorts config.NumThreads (fun portIndex threadIndex -> new Thread(new ThreadStart(threadFn portIndex threadIndex data)))
                |> Array2D.iter (fun x -> x.Start())
        } 
    
    let onOpen portIndex (_ : EventArgs) : unit =
        Log.Information("Websocket connected!")
        isReady.[portIndex] <- true
        if not heartbeat.[portIndex].IsAlive then heartbeat.[portIndex].Start()

    let onClose portIndex (_ : EventArgs) : unit =
        Log.Information("Websocket closed!")
        isReady.[portIndex] <- false

    let onError (args : SuperSocket.ClientEngine.ErrorEventArgs) : unit =
        Log.Error(args.Exception, "Websocket error!")

    let onDataReceived portIndex (args: DataReceivedEventArgs) : unit =
        Log.Debug("Data received")
        Interlocked.Increment(&dataMsgCount) |> ignore
        try
            data.[portIndex].Add(args.Data, ctSource.Token)
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
            ws.Closed.Add(onClose i)
            ws.Error.Add(onError)
            ws.DataReceived.Add(onDataReceived i)
            ws.MessageReceived.Add(onMessageReceived)
            ws.Open()
            ws
            )

        

    member this.Join() : unit =
        while not(isReady |> Array.fold (&&) true) do Thread.Sleep(1000)
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
        isReady <-  isReady |> Array.map (fun _ -> false)
        ctSource.Cancel()
        for ws in wss do ws.Close()
        for h in heartbeat do h.Join()
        //for thread in threads do thread.Join()
        Log.Information("Stopped")

    static member Log(messageTemplate:string, [<ParamArray>] propertyValues:obj[]) = Log.Information(messageTemplate, propertyValues)