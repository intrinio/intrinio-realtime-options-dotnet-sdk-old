using System;
using System.Threading;
using System.Collections.Concurrent;
using Intrinio;

namespace SampleApp
{
	class Program
	{
		private static Client client = null;
		private static Timer timer = null;
		private static readonly ConcurrentDictionary<string, int> symbols = new ConcurrentDictionary<string, int>(5, 1_500_000);
		private static int maxCount = 0;
		private static Quote maxCountQuote;
		private static readonly object obj = new object();
		
		static void OnQuote(Quote quote)
		{
			string key = quote.Symbol + ":" + quote.Type;
			if (!symbols.ContainsKey(key)) {
				symbols[key] = 1;
			} else
			{
				symbols[key]++;
			}
			if (symbols[key] > maxCount)
			{
				lock (obj)
				{
					maxCount++;
					maxCountQuote = quote;
				}
			}
		}

		static void TimerCallback(object obj)
		{
			Client.Log("Most active symbol: {0} ({1} updates)", maxCountQuote.Symbol, maxCount);
			if (!maxCountQuote.Equals(new Quote())) {
				Quote quote = maxCountQuote;
				Client.Log("{0} (strike price = {1}, isPut = {2}, isCall = {3}, expiration = {4})", quote.Symbol, quote.GetStrikePrice(), quote.IsPut(), quote.IsCall(), quote.GetExpirationDate());
			}
		}

		static void Cancel(object sender, ConsoleCancelEventArgs args)
		{
			Client.Log("Stopping sample app");
			timer.Dispose();
			client.Stop();
			Environment.Exit(0);
		}

		static void Main(string[] args)
		{
			Client.Log("Starting sample app");
			timer = new Timer(TimerCallback, null, 10000, 10000);
			client = new Client(OnQuote);
			client.Join();
			Console.CancelKeyPress += new ConsoleCancelEventHandler(Cancel);
		}

		
	}
}
