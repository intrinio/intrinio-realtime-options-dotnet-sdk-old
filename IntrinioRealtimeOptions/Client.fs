﻿namespace Intrinio

open Serilog
open System
open System.IO
open System.Net
open System.Text
open System.Text.Json
open System.Collections.Concurrent
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open System.Net.Sockets
open WebSocket4Net
open Intrinio.Config

type internal WebSocketState(ws: WebSocket) =
    let mutable webSocket : WebSocket = ws
    let mutable isReady : bool = false
    let mutable isReconnecting : bool = false
    let mutable lastReset : DateTime = DateTime.Now

    member _.WebSocket
        with get() : WebSocket = webSocket
        and set (ws:WebSocket) = webSocket <- ws

    member _.IsReady
        with get() : bool = isReady
        and set (ir:bool) = isReady <- ir

    member _.IsReconnecting
        with get() : bool = isReconnecting
        and set (ir:bool) = isReconnecting <- ir

    member _.LastReset : DateTime = lastReset

    member _.Reset() : unit = lastReset <- DateTime.Now

type Client(onTrade : Action<Trade>, onQuote : Action<Quote>, onOpenInterest : Action<OpenInterest>) =
    let [<Literal>] heartbeatMessage : string = "{\"topic\":\"phoenix\",\"event\":\"heartbeat\",\"payload\":{},\"ref\":null}"
    let [<Literal>] heartbeatResponse : string = "{\"topic\":\"phoenix\",\"ref\":null,\"payload\":{\"status\":\"ok\",\"response\":{}},\"event\":\"phx_reply\"}"
    let [<Literal>] errorResponse : string = "\"status\":\"error\""
    let selfHealBackoffs : int[] = [| 10_000; 30_000; 60_000; 300_000; 600_000 |]

    let config = LoadConfig()
    let tLock : ReaderWriterLockSlim = new ReaderWriterLockSlim()
    let wsLock : ReaderWriterLockSlim = new ReaderWriterLockSlim()
    let mutable token : (string * DateTime) = (null, DateTime.Now)
    let mutable wsStates : WebSocketState[] = Array.empty<WebSocketState>
    let mutable dataMsgCount : int64 = 0L
    let mutable textMsgCount : int64 = 0L
    let channels : HashSet<(string*bool)> = new HashSet<(string*bool)>()
    let ctSource : CancellationTokenSource = new CancellationTokenSource()
    let data : BlockingCollection<byte[]> = new BlockingCollection<byte[]>(new ConcurrentQueue<byte[]>())
    let mutable tryReconnect : (int -> unit -> unit) = fun (_:int) () -> ()

    let allReady() : bool = 
        wsLock.EnterReadLock()
        try wsStates |> Array.forall (fun (wss:WebSocketState) -> wss.IsReady)
        finally wsLock.ExitReadLock()

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

    let parseTrade (bytes: ReadOnlySpan<byte>) : Trade =
        {
            Symbol = Encoding.ASCII.GetString(bytes.Slice(0, 21))
            Price = BitConverter.ToDouble(bytes.Slice(22, 8))
            Size = BitConverter.ToUInt32(bytes.Slice(30, 4))
            Timestamp = BitConverter.ToDouble(bytes.Slice(34, 8))
            TotalVolume = BitConverter.ToUInt64(bytes.Slice(42, 8))
        }

    let parseQuote (bytes: ReadOnlySpan<byte>) : Quote =
        {
            Symbol = Encoding.ASCII.GetString(bytes.Slice(0, 21))
            Type = enum<QuoteType> (int32 (bytes.Item(21)))
            Price = BitConverter.ToDouble(bytes.Slice(22, 8))
            Size = BitConverter.ToUInt32(bytes.Slice(30, 4))
            Timestamp = BitConverter.ToDouble(bytes.Slice(34, 8))
        }

    let parseOpenInterest (bytes: ReadOnlySpan<byte>) : OpenInterest =
        {
            Symbol = Encoding.ASCII.GetString(bytes.Slice(0, 21))
            OpenInterest = BitConverter.ToInt32(bytes.Slice(22, 4))
            Timestamp = BitConverter.ToDouble(bytes.Slice(26, 8))
        }

    let parseSocketMessage (bytes: byte[], startIndex: byref<int>) : unit =
        let msgType = enum<MessageType> (int32 bytes.[startIndex + 21])
        match msgType with
        | MessageType.Trade -> 
            let chunk: ReadOnlySpan<byte> = new ReadOnlySpan<byte>(bytes, startIndex, 50)
            let trade: Trade = parseTrade(chunk)
            startIndex <- startIndex + 50
            trade |> onTrade.Invoke
        | MessageType.Ask | MessageType.Bid -> 
            let chunk: ReadOnlySpan<byte> = new ReadOnlySpan<byte>(bytes, startIndex, 42)
            let quote: Quote = parseQuote(chunk)
            startIndex <- startIndex + 42
            quote |> onQuote.Invoke
        | MessageType.OpenInterest -> 
            let chunk: ReadOnlySpan<byte> = new ReadOnlySpan<byte>(bytes, startIndex, 34)
            let openInterest = parseOpenInterest(chunk)
            startIndex <- startIndex + 34
            openInterest |> onOpenInterest.Invoke
        | _ -> Log.Warning("Invalid MessageType: {0}", (int32 bytes.[startIndex + 21]))

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
        let mutable datum : byte[] = Array.empty<byte>
        while not (ct.IsCancellationRequested) do
            try
                if data.TryTake(&datum,1000) then
                    // These are grouped (many) messages.
                    // The first byte tells us how many there are.
                    // From there, check the type at index 21 to know how many bytes each message has.
                    let cnt = datum.[0] |> int
                    let mutable startIndex = 1
                    for _ in 1 .. cnt do
                        parseSocketMessage(datum, &startIndex)
            with :? OperationCanceledException -> ()

    let threads : Thread[] = Array.init config.NumThreads (fun _ -> new Thread(new ThreadStart(threadFn)))

    let doBackoff(fn: unit -> bool) : unit =
        let mutable i : int = 0
        let mutable backoff : int = selfHealBackoffs.[i]
        let mutable success : bool = fn()
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
                | _ ->
                    Log.Warning("Authorization Failure {0}: The authorization key you provided is likely incorrect.", response.StatusCode.ToString())
                    false
        with
        | :? WebException ->
            Log.Error("Authorization Failure. The authorization server is likey offline.")
            false
        | :? IOException ->
            Log.Error("Authorization Failure. Please check your network connection.")
            false
        | _ as exn ->
            Log.Error("Unidentified Authorization Failure: {0}:{1}", exn.GetType(), exn.Message)
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
            wsStates.[index].IsReconnecting <- false
            if not heartbeat.IsAlive
            then heartbeat.Start()
            for thread in threads do
                if not thread.IsAlive
                then thread.Start()
        finally wsLock.ExitWriteLock()
        if channels.Count > 0
        then
            channels |> Seq.iter (fun (symbol: string, tradesOnly:bool) ->
                let lastOnly : string = if tradesOnly then "true" else "false" 
                let message : string = "{\"topic\":\"options:" + symbol + "\",\"event\":\"phx_join\",\"last_only\":\"" + lastOnly + "\",\"payload\":{},\"ref\":null}"
                Log.Information("Websocket {0} - Joining channel: {1:l} (trades only = {2:l})", index, symbol, lastOnly)
                wsStates.[index].WebSocket.Send(message) )

    let onClose (index : int) (_ : EventArgs) : unit =
        wsLock.EnterUpgradeableReadLock()
        try 
            if not wsStates.[index].IsReconnecting
            then
                Log.Information("Websocket {0} - Closed", index)
                wsLock.EnterWriteLock()
                try wsStates.[index].IsReady <- false
                finally wsLock.ExitWriteLock()
                if (not ctSource.IsCancellationRequested)
                then Task.Factory.StartNew(Action(tryReconnect(index))) |> ignore
        finally wsLock.ExitUpgradeableReadLock()

    let (|Closed|Refused|Unavailable|Other|) (input:exn) =
        if (input.GetType() = typeof<SocketException>) &&
            input.Message.StartsWith("A connection attempt failed because the connected party did not properly respond after a period of time")
        then Closed
        elif (input.GetType() = typeof<SocketException>) &&
            (input.Message = "No connection could be made because the target machine actively refused it.")
        then Refused
        elif input.Message.StartsWith("HTTP/1.1 503")
        then Unavailable
        else Other

    let onError (index : int) (args : SuperSocket.ClientEngine.ErrorEventArgs) : unit =
        let exn = args.Exception
        match exn with
        | Closed -> Log.Warning("Websocket {0} - Error - Connection failed", index)
        | Refused -> Log.Warning("Websocket {0} - Error - Connection refused", index)
        | Unavailable -> Log.Warning("Websocket {0} - Error - Server unavailable", index)
        | _ -> Log.Error("Websocket {0} - Error - {1}:{2}", index, exn.GetType(), exn.Message)

    let onDataReceived (index : int) (args: DataReceivedEventArgs) : unit =
        Log.Debug("Websocket {0} - Data received", index)
        Interlocked.Increment(&dataMsgCount) |> ignore
        data.Add(args.Data)

    let onMessageReceived (index : int) (args : MessageReceivedEventArgs) : unit =
        Log.Debug("Websocket {0} - Message received", index)
        Interlocked.Increment(&textMsgCount) |> ignore
        if args.Message = heartbeatResponse then Log.Debug("Heartbeat response received")
        elif args.Message.Contains(errorResponse)
        then
            let replyDoc : JsonDocument = JsonDocument.Parse(args.Message)
            let errorMessage : string = 
                replyDoc.RootElement
                    .GetProperty("payload")
                    .GetProperty("response")
                    .GetString()
            Log.Error("Error received: {0:l}", errorMessage)

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

    let join(symbol: string, tradesOnly: bool) : unit =
        let lastOnly : string = if tradesOnly then "true" else "false"
        if channels.Add((symbol, tradesOnly))
        then 
            let message : string = "{\"topic\":\"options:" + symbol + "\",\"event\":\"phx_join\",\"last_only\":\"" + lastOnly + "\",\"payload\":{},\"ref\":null}"
            wsStates |> Array.iteri (fun (index:int) (wss:WebSocketState) ->
                Log.Information("Websocket {0} - Joining channel: {1:l} (trades only = {2:l})", index, symbol, lastOnly)
                try wss.WebSocket.Send(message)
                with _ -> channels.Remove((symbol, tradesOnly)) |> ignore )

    let leave(symbol: string, tradesOnly: bool) : unit =
        let lastOnly : string = if tradesOnly then "true" else "false"
        if channels.Remove((symbol, tradesOnly))
        then 
            let message : string = "{\"topic\":\"options:" + symbol + "\",\"event\":\"phx_leave\",\"last_only\":\"" + lastOnly + "\",\"payload\":{},\"ref\":null}"
            wsStates |> Array.iteri (fun (index:int) (wss:WebSocketState) ->
                Log.Information("Websocket {0} - Leaving channel: {1:l} (trades only = {2})", index, symbol, lastOnly)
                try wss.WebSocket.Send(message)
                with _ -> () )

    do
        tryReconnect <- fun (index:int) () ->
            let reconnectFn () : bool =
                Log.Information("Websocket {0} - Reconnecting...", index)
                if wsStates.[index].IsReady then true
                else
                    wsLock.EnterWriteLock()
                    try wsStates.[index].IsReconnecting <- true
                    finally wsLock.ExitWriteLock()
                    if (DateTime.Now - TimeSpan.FromDays(5.0)) > (wsStates.[index].LastReset)
                    then
                        let _token : string = getToken()
                        resetWebSocket(index, _token)
                    else
                        try
                            wsStates.[index].WebSocket.Open()
                        with _ -> ()
                    false
            doBackoff(reconnectFn)
        let _token : string = getToken()
        initializeWebSockets(_token)

    new (onTrade : Action<Trade>, onQuote : Action<Quote>) =
        Client(onTrade, onQuote, Action<OpenInterest>(fun (_:OpenInterest) -> ()))

    new (onTrade : Action<Trade>) =
        Client(onTrade, Action<Quote>(fun (_:Quote) -> ()), Action<OpenInterest>(fun (_:OpenInterest) -> ()))

    member _.Join() : unit =
        while not(allReady()) do Thread.Sleep(1000)
        let symbolsToAdd : HashSet<(string*bool)> =
            config.Symbols
            |> Seq.map(fun (symbol:string) -> (symbol, config.TradesOnly))
            |> fun (symbols:seq<(string*bool)>) -> new HashSet<(string*bool)>(symbols)
        symbolsToAdd.ExceptWith(channels)
        for symbol in symbolsToAdd do join(symbol)

    member _.Join(symbol: string, ?tradesOnly: bool) : unit =
        let t: bool =
            match tradesOnly with
            | Some(v:bool) -> v || config.TradesOnly
            | None -> false || config.TradesOnly
        while not(allReady()) do Thread.Sleep(1000)
        if not (channels.Contains((symbol, t)))
        then join(symbol, t)

    member _.Join(symbols: string[], ?tradesOnly: bool) : unit =
        let t: bool =
            match tradesOnly with
            | Some(v:bool) -> v || config.TradesOnly
            | None -> false || config.TradesOnly
        while not(allReady()) do Thread.Sleep(1000)
        let symbolsToAdd : HashSet<(string*bool)> =
            symbols
            |> Seq.map(fun (symbol:string) -> (symbol,t))
            |> fun (_symbols:seq<(string*bool)>) -> new HashSet<(string*bool)>(_symbols)
        symbolsToAdd.ExceptWith(channels)
        for symbol in symbolsToAdd do join(symbol)

    member _.Leave() : unit =
        for channel in channels do leave(channel)

    member _.Leave(symbol: string) : unit =
        let matchingChannels : seq<(string*bool)> = channels |> Seq.where (fun (_symbol:string, _:bool) -> _symbol = symbol)
        for channel in matchingChannels do leave(channel)

    member _.Leave(symbols: string[]) : unit =
        let _symbols : HashSet<string> = new HashSet<string>(symbols)
        let matchingChannels : seq<(string*bool)> = channels |> Seq.where(fun (symbol:string, _:bool) -> _symbols.Contains(symbol))
        for channel in matchingChannels do leave(channel)

    member _.Stop() : unit =
        for channel in channels do leave(channel)
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

    member _.GetStats() : (int64 * int64 * int) = (Interlocked.Read(&dataMsgCount), Interlocked.Read(&textMsgCount), data.Count)

    static member Log(messageTemplate:string, [<ParamArray>] propertyValues:obj[]) = Log.Information(messageTemplate, propertyValues)


