using BenchmarkDotNet.Running;
using FASTER.core;
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading.Tasks;
using Xunit;

namespace Repro.Faster.MemoryAndSpanByteReadPerf
{
    public class Program
    {
        public static async Task Main()
        {
            await ProfileStore(new ObjLogMinimalKVStore());
        }

        private static async Task ProfileStore(IMinimalKVStore minimalKVStore)
        {
            Console.WriteLine($"Profiling {minimalKVStore.GetType().Name}:");
            Stopwatch stopWatch = new Stopwatch();
            const int numOperations = 509;

            // Insert
            Console.WriteLine($"    Inserting {numOperations} records ...");
            stopWatch.Start();
            Parallel.For(0, numOperations, key => minimalKVStore.Upsert(key, key));
            stopWatch.Stop();
            Console.WriteLine($"    Insertion complete in {stopWatch.ElapsedMilliseconds} ms");

            // Read
            ConcurrentDictionary<long, Task<(Status, int?)>> readTasks = new();
            Console.WriteLine($"    Reading {numOperations} records ...");
            stopWatch.Restart();
            Parallel.For(0, numOperations, key => readTasks.TryAdd(key, minimalKVStore.ReadAsync(key)));
            await Task.WhenAll(readTasks.Values).ConfigureAwait(false);
            stopWatch.Stop();
            Console.WriteLine($"    Reads complete in {stopWatch.ElapsedMilliseconds} ms");

            // Verify
            Console.WriteLine("    Verifying read results ...");
            Parallel.For(0, numOperations, key =>
            {
                (Status status, int? result) = readTasks[key].Result;
                Assert.Equal(Status.OK, status);
                Assert.Equal(key, result);
            });

            Console.WriteLine("    Results verified");

            minimalKVStore.Dispose();
        }
    }
}
