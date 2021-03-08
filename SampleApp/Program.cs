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
		private static ConcurrentDictionary<string, int> symbols = new ConcurrentDictionary<string, int>(5, 1_500_000);
		private static int maxCount = 0;
		private static string maxCountSymbol = null;
		
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
				Interlocked.Increment(ref maxCount);
				Interlocked.Exchange(ref maxCountSymbol, key);
			}
		}

		static void TimerCallback(object obj)
		{
			Console.WriteLine("Most active symbol: {0} ({1} updates)", maxCountSymbol, maxCount);
		}

		static void Cancel(object sender, ConsoleCancelEventArgs args)
		{
			Console.WriteLine("Stopping sample app");
			timer.Dispose();
			client.Stop();
		}

		static void Main(string[] args)
		{
			Console.WriteLine("Starting sample app");
			timer = new Timer(TimerCallback, null, 10000, 10000);
			client = new Client(OnQuote);
			client.Join();
			Console.CancelKeyPress += new ConsoleCancelEventHandler(Cancel);
		}

		
	}
}
