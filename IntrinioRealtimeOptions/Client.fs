﻿namespace Intrinio

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

type internal WebSocketState(ws: WebSocket) =
    let mutable webSocket : WebSocket = ws
    let mutable isReady : bool = false
    let mutable lastReset : DateTime = DateTime.Now

    member _.WebSocket
        with get() : WebSocket = webSocket
        and set (ws:WebSocket) = webSocket <- ws

    member _.IsReady
        with get() : bool = isReady
        and set (ir:bool) = isReady <- ir

    member _.LastReset : DateTime = lastReset

    member _.Reset() : unit = lastReset <- DateTime.Now

type Client(onQuote : Action<Quote>) =
    let [<Literal>] heartbeatMessage : string = "{\"topic\":\"phoenix\",\"event\":\"heartbeat\",\"payload\":{},\"ref\":null}"
    let [<Literal>] heartbeatResponse : string = "{\"topic\":\"phoenix\",\"ref\":null,\"payload\":{\"status\":\"ok\",\"response\":{}},\"event\":\"phx_reply\"}"
    let [<Literal>] heartbeatErrorResponse : string = "\"status\":\"error\""
    let selfHealBackoffs : int[] = [| 10_000; 30_000; 60_000; 300_000; 600_000 |]

    let config = LoadConfig()
    let tLock : ReaderWriterLockSlim = new ReaderWriterLockSlim()
    let wsLock : ReaderWriterLockSlim = new ReaderWriterLockSlim()
    let mutable token : (string * DateTime) = (null, DateTime.Now)
    let mutable wsStates : WebSocketState[] = Array.empty<WebSocketState>
    let mutable dataMsgCount : int64 = 0L
    let mutable textMsgCount : int64 = 0L
    let channels : LinkedList<string> = new LinkedList<string>()
    let ctSource : CancellationTokenSource = new CancellationTokenSource()
    let data : BlockingCollection<byte[]> = new BlockingCollection<byte[]>(new ConcurrentQueue<byte[]>())
    let mutable tryReconnect : (int -> unit) = fun (_:int) -> ()

    let statsTimer : Timer = new Timer(new TimerCallback(fun (_:obj) ->
        Log.Information("Messages received: (data = {0}, text = {1}, queue depth = {2})", dataMsgCount, textMsgCount, data.Count)
        ), null, Timeout.Infinite, Timeout.Infinite)

    let getAuthUrl () : string =
        match config.Provider with
        | Provider.OPRA -> "https://realtime-options.intrinio.com/auth?api_key=" + config.ApiKey
        | Provider.OPRA_FIREHOSE -> "https://realtime-options-firehose.intrinio.com:8000/auth?api_key=" + config.ApiKey
        | Provider.MANUAL -> "http://" + config.IPAddress + "/auth?api_key=" + config.ApiKey
        | Provider.MANUAL_FIREHOSE -> "http://" + config.IPAddress + ":8000/auth?api_key=" + config.ApiKey
        | _ -> failwith "Provider not specified!"

    let getWebSocketUrl (token: string, index: int) : string =
        match config.Provider with
        | Provider.OPRA -> "wss://realtime-options.intrinio.com/socket/websocket?vsn=1.0.0&token=" + token
        | Provider.OPRA_FIREHOSE -> "wss://realtime-options-firehose.intrinio.com:800" + index.ToString() + "/socket/websocket?vsn=1.0.0&token=" + token
        | Provider.MANUAL -> "ws://" + config.IPAddress + "/socket/websocket?vsn=1.0.0&token=" + token
        | Provider.MANUAL_FIREHOSE -> "ws://" + config.IPAddress + ":800" + index.ToString() + "/socket/websocket?vsn=1.0.0&token=" + token
        | _ -> failwith "Provider not specified!"

    let getWebSocketCount () : int =
        match config.Provider with
        | Provider.OPRA -> 1
        | Provider.OPRA_FIREHOSE -> 6
        | Provider.MANUAL -> 1
        | Provider.MANUAL_FIREHOSE -> 6
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
        Log.Debug("Starting heartbeat")
        while not(ct.IsCancellationRequested) do
            Thread.Sleep(20000) //send heartbeat every 20 sec
            Log.Debug("Sending heartbeat")
            wsLock.EnterReadLock()
            try
                wsStates |> Array.iter (fun (wss: WebSocketState) ->
                    if not(ct.IsCancellationRequested) && wss.IsReady
                    then wss.WebSocket.Send(heartbeatMessage) )
            finally wsLock.ExitReadLock()

    let heartbeat : Thread = new Thread(new ThreadStart(heartbeatFn))

    let threadFn () : unit =
        let ct = ctSource.Token
        while not (ct.IsCancellationRequested) do
            try
                let mutable datum : byte[] = Array.empty<byte>
                if data.TryTake(&datum, 1000)
                then
                    let quote : Quote = parseMessage(datum)
                    Log.Debug("Invoking 'onQuote'")
                    onQuote.Invoke(quote)
            with :? OperationCanceledException -> ()

    let threads : Thread[] = Array.init config.NumThreads (fun _ -> new Thread(new ThreadStart(threadFn)))

    let doBackoff(fn: unit -> bool) : unit =
        let mutable i : int = 0
        let mutable backoff : int = selfHealBackoffs.[i]
        let mutable success : bool = false
        while not success do 
            Thread.Sleep(backoff)
            i <- Math.Min(i + 1, selfHealBackoffs.Length - 1)
            backoff <- selfHealBackoffs.[i]
            success <- fn()

    let trySetToken() : bool =
        Log.Information("Authorizing...")
        try
            let authUrl : string = getAuthUrl()
            HttpWebRequest.Create(authUrl).GetResponse() :?> HttpWebResponse
            |> fun response ->
                match response.StatusCode with
                | HttpStatusCode.OK ->
                    let stream : Stream = response.GetResponseStream()
                    let reader : StreamReader = new StreamReader(stream, Encoding.UTF8)
                    let _token : string = reader.ReadToEnd()
                    Interlocked.Exchange(&token, (_token, DateTime.Now)) |> ignore
                    Log.Information("Authorization successful")
                    true
                | _ -> raise (AccessViolationException("Authorization Failure: " + response.StatusCode.ToString()))
        with
        | :? WebException | :? IOException as exn ->
            Log.Error("Authorization Failure: {0}. The authorization server is likey offline. Please make sure you're trying to connect during market hours.", exn.Message)
            false
        | :? AccessViolationException as exn ->
            Log.Error("{0). The authorization key you provided is likely incorrect.", exn.Message)
            false
        | _ as exn ->
            Log.Error("Unidentified Authorization Failure: {0}", exn.Message)
            false

    let getToken() : string =
        tLock.EnterUpgradeableReadLock()
        try
            if (DateTime.Now - TimeSpan.FromDays(1.0)) > (snd token)
            then (fst token)
            else
                tLock.EnterWriteLock()
                try doBackoff(trySetToken)
                finally tLock.ExitWriteLock()
                fst token
        finally tLock.ExitUpgradeableReadLock()

    let onOpen (index : int) (_ : EventArgs) : unit =
        Log.Information("Websocket {0} - Connected", index)
        wsLock.EnterWriteLock()
        try
            wsStates.[index].IsReady <- true
            if not heartbeat.IsAlive
            then heartbeat.Start()
            for thread in threads do
                if not thread.IsAlive
                then thread.Start()
        finally wsLock.ExitWriteLock()
        if channels.Count > 0
        then
            channels |> Seq.iter (fun (symbol: string) ->
                let message = "{\"topic\":\"options:" + symbol + "\",\"event\":\"phx_join\",\"payload\":{},\"ref\":null}"
                Log.Information("Websocket {0} - Joining channel: {1}", index, symbol)
                wsStates.[index].WebSocket.Send(message) )

    let onClose (index : int) (_ : EventArgs) : unit =
        Log.Information("Websocket {0} - Closed", index)
        wsLock.EnterWriteLock()
        try wsStates.[index].IsReady <- false
        finally wsLock.ExitWriteLock()
        if not ctSource.IsCancellationRequested
        then tryReconnect(index)

    let onError (index : int) (args : SuperSocket.ClientEngine.ErrorEventArgs) : unit =
        let exn = args.Exception
        Log.Error(exn, "Websocket {0} - Error - {1}:{2}", index, exn.GetType(), exn.Message)

    let onDataReceived (index : int) (args: DataReceivedEventArgs) : unit =
        Log.Debug("Websocket {0} - Data received", index)
        Interlocked.Increment(&dataMsgCount) |> ignore
        data.Add(args.Data)

    let onMessageReceived (index : int) (args : MessageReceivedEventArgs) : unit =
        Log.Debug("Websocket {0} - Message received", index)
        Interlocked.Increment(&textMsgCount) |> ignore
        if args.Message = heartbeatResponse then Log.Debug("Heartbeat response received")
        elif args.Message.Contains(heartbeatErrorResponse) then Log.Error("Heartbeat error received")

    let resetWebSocket(index: int, token: string) : unit =
        Log.Information("Websocket {0} - Resetting", index)
        let wsUrl : string = getWebSocketUrl(token, index)
        let ws : WebSocket = new WebSocket(wsUrl)
        ws.Opened.Add(onOpen index)
        ws.Closed.Add(onClose index)
        ws.Error.Add(onError index)
        ws.DataReceived.Add(onDataReceived index)
        ws.MessageReceived.Add(onMessageReceived index)
        wsLock.EnterWriteLock()
        try
            wsStates.[index].WebSocket <- ws
            wsStates.[index].Reset()
        finally wsLock.ExitWriteLock()
        ws.Open()

    let initializeWebSockets(token: string) : unit =
        wsLock.EnterWriteLock()
        try
            let wsCount : int = getWebSocketCount()
            wsStates <- Array.init wsCount (fun (index:int) ->
                Log.Information("Websocket {0} - Connecting...", index)
                let wsUrl : string = getWebSocketUrl(token, index)
                let ws: WebSocket = new WebSocket(wsUrl)
                ws.Opened.Add(onOpen index)
                ws.Closed.Add(onClose index)
                ws.Error.Add(onError index)
                ws.DataReceived.Add(onDataReceived index)
                ws.MessageReceived.Add(onMessageReceived index)
                new WebSocketState(ws) )
        finally wsLock.ExitWriteLock()
        wsStates |> Array.iter (fun (wss: WebSocketState) -> wss.WebSocket.Open())

    do
        tryReconnect <- fun (index:int) ->
            Log.Information("Websocket {0} - Reconnecting...", index)
            let reconnectFn () : bool =
                if wsStates.[index].IsReady then true
                else
                    if (DateTime.Now - TimeSpan.FromDays(5.0)) > (wsStates.[index].LastReset)
                    then
                        let _token : string = getToken()
                        resetWebSocket(index, _token)
                    else
                        try wsStates.[index].WebSocket.Open()
                        with _ -> ()
                    false
            doBackoff(reconnectFn)
        let _token : string = getToken()
        initializeWebSockets(_token)
        statsTimer.Change(30_000, 30_000) |> ignore

    member this.Join() : unit =
        let allReady() = wsStates |> Array.forall (fun (wss:WebSocketState) -> wss.IsReady)
        while not(allReady()) do Thread.Sleep(1000)
        for symbol in config.Symbols do
            channels.AddLast(symbol) |> ignore
            let message = "{\"topic\":\"options:" + symbol + "\",\"event\":\"phx_join\",\"payload\":{},\"ref\":null}"
            wsStates |> Array.iteri (fun (index:int) (wss:WebSocketState) ->
                Log.Information("Websocket {0} - Joining channel: {1}", index, symbol)
                wss.WebSocket.Send(message) )

    member this.Stop() : unit =
        while channels.Count > 0 do
            let channel = channels.First.Value
            channels.RemoveFirst()
            let message = "{\"topic\":\"options:" + channel + "\",\"event\":\"phx_leave\",\"payload\":{},\"ref\":null}"
            wsStates |> Array.iteri (fun (index:int) (wss:WebSocketState) ->
                Log.Information("Websocket {0} - Leaving channel: {1}", index, channel)
                wss.WebSocket.Send(message) )
        Thread.Sleep(1000)
        wsLock.EnterWriteLock()
        try wsStates |> Array.iter (fun (wss:WebSocketState) -> wss.IsReady <- false)
        finally wsLock.ExitWriteLock()
        ctSource.Cancel ()
        wsStates |> Array.iteri (fun (index:int) (wss:WebSocketState) ->
            Log.Information("Websocket {0} - Closing...", index);
            wss.WebSocket.Close())
        heartbeat.Join()
        for thread in threads do thread.Join()
        Log.Information("Stopped")

    static member Log(messageTemplate:string, [<ParamArray>] propertyValues:obj[]) = Log.Information(messageTemplate, propertyValues)


