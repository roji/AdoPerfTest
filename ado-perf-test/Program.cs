using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace ado_perf_test
{
    public class Program
    {
        const int WarmupTimeSeconds = 3;
        static string Url;

        static int _isRunning = 1;
        static long _counter;

        static void Main(string[] args)
        {
            if (args.Length != 4)
                throw new Exception("usage: Main <PG JDBC URL> <threads> <seconds> <sync or async>");
            Url = args[0];
            var threadCount = int.Parse(args[1]);
            var seconds = int.Parse(args[2]);

            switch (args[3])
            {
            case "sync":
                Sync(threadCount, seconds);
                return;
            case "async":
                Async(threadCount, seconds);
                break;
            default:
                throw new Exception("usage: Main <PG JDBC URL> <threads> <seconds> <sync or async>");
            }
        }

        static void Sync(int threadCount, int seconds)
        {
            var threads = Enumerable.Range(0, threadCount)
                .Select(i => new Thread(NonCaching))
                .ToList();

            foreach (var thread in threads)
                thread.Start();

            Console.WriteLine("Warming up...");
            Thread.Sleep(WarmupTimeSeconds * 1000);

            Console.WriteLine("Starting benchmark...");
            Interlocked.Exchange(ref _counter, 0);
            Thread.Sleep(seconds * 1000);
            Interlocked.Exchange(ref _isRunning, 0);
            var transactions = _counter;
            Console.WriteLine("Finished");

            foreach (var thread in threads)
                thread.Join();

            Console.WriteLine($"Ran {transactions} in {seconds} seconds");
            Console.WriteLine($"Transactions per second: {transactions / seconds}");
        }

        static void Async(int threadCount, int seconds)
        {
            var tasks = Enumerable.Range(0, threadCount)
                .Select(i => Task.Run(NonCachingAsync))
                .ToArray();

            Console.WriteLine("Warming up...");
            Thread.Sleep(WarmupTimeSeconds * 1000);

            Console.WriteLine("Starting benchmark...");
            Interlocked.Exchange(ref _counter, 0);
            Thread.Sleep(seconds * 1000);
            Interlocked.Exchange(ref _isRunning, 0);
            var transactions = _counter;
            Console.WriteLine("Finished");

            Task.WaitAll(tasks);

            Console.WriteLine($"Ran {transactions} in {seconds} seconds");
            Console.WriteLine($"Transactions per second: {transactions / seconds}");
        }

        static void NonCaching()
        {
            while (true)
            {
                var results = new List<Fortune>();

                using (var conn = new NpgsqlConnection(Url))
                using (var cmd = new NpgsqlCommand())
                {
                    conn.Open();
                    cmd.Connection = conn;
                    cmd.CommandText = "SELECT id, message FROM fortune";
                    cmd.Prepare();
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            results.Add(
                                new Fortune
                                {
                                    Id = reader.GetInt32(0),
                                    Message = reader.GetString(1)
                                });
                        }
                    }
                    if (results.Count != 12)
                        throw new InvalidOperationException($"Unexpected number of results! Expected 12 got {results.Count}");
                    
                    Interlocked.Increment(ref _counter);
                    if (_isRunning == 0)
                        return;
                }
            }
        }

        static async Task NonCachingAsync()
        {
            while (true)
            {
                var results = new List<Fortune>();

                using (var conn = new NpgsqlConnection(Url))
                using (var cmd = new NpgsqlCommand())
                {
                    await conn.OpenAsync();
                    cmd.Connection = conn;
                    cmd.CommandText = "SELECT id, message FROM fortune";
                    cmd.Prepare();
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            results.Add(
                                new Fortune
                                {
                                    Id = reader.GetInt32(0),
                                    Message = reader.GetString(1)
                                });
                        }
                    }
                    if (results.Count != 12)
                        throw new InvalidOperationException($"Unexpected number of results! Expected 12 got {results.Count}");
                    
                    Interlocked.Increment(ref _counter);
                    if (_isRunning == 0)
                        return;
                }
            }
        }
    }

    public class Fortune
    {
        public int Id { get; set; }
        public string Message { get; set; }
    }
}
