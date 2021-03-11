using FASTER.core;
using System;
using System.Threading.Tasks;

namespace Repro.Faster.MemoryAndSpanByteReadPerf
{
    public interface IMinimalKVStore : IDisposable
    {
        int NumSessions { get; }
        Task<(Status, int?)> ReadAsync(int key);
        void Upsert(int key, int value);
    }
}