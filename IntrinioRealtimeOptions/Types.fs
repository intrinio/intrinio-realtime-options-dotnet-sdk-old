namespace Intrinio

type Provider =
    | NONE = 0
    | OPRA = 1
    | OPRA_FIREHOSE = 2

/// A 'Quote' is the standard unit of data representing an individual market event. A quote object will be returned for every market transaction.
/// Type: the type of the quote (will always be 'trade' for firehose data)
/// Symbol: the id of the option contract (e.g. AAPL210305C00070000)
/// Price: the dollar price of the last trade </para>
/// Volume: the number of contacts that were exchanged in the last trade </para>
/// Timestamp: the time that the trade was executed (a unix timestamp representing the number of milliseconds (or better) since the unix epoch) </para>
type [<Struct>] Quote =
    {
        Type : string 
        Symbol : string
        Price : float32
        Size : uint32
        Timestamp : float32
    }
    override this.ToString() : string =
        "Quote (" +
        "Type: " + this.Type +
        ", Symbol: " + this.Symbol +
        ", Price: " + this.Price.ToString() +
        ", Size: " + this.Size.ToString() +
        ", Timestamp: " + this.Timestamp.ToString() +
        ")"