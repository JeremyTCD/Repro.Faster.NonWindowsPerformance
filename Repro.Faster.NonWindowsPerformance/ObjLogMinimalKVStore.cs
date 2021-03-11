using FASTER.core;
using MessagePack;
using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading.Tasks;

namespace Repro.Faster.MemoryAndSpanByteReadPerf
{
    public class ObjLogMinimalKVStore : IMinimalKVStore
    {
        private readonly SimpleFunctions<int, int, Empty> _simpleFunctions = new();
        private readonly ConcurrentQueue<ClientSession<int, int, int, int, Empty, SimpleFunctions<int, int, Empty>>> _sessionPool = new();
        private readonly FasterKV<int, int> _kvStore;
        private readonly FasterKV<int, int>.ClientSessionBuilder<int, int, Empty> _clientSessionBuilder;

        public int NumSessions => _sessionPool.Count;

        public ObjLogMinimalKVStore()
        {
            // Settings
            string logDirectory = Path.Combine(Path.GetTempPath(), "FasterLogs");
            string logFileName = Guid.NewGuid().ToString();
            var logSettings = new LogSettings
            {
                LogDevice = Devices.CreateLogDevice(Path.Combine(logDirectory, $"{logFileName}.log"), deleteOnClose: true),
                ObjectLogDevice = Devices.CreateLogDevice(Path.Combine(logDirectory, $"{logFileName}.obj.log"), deleteOnClose: true),
                PageSizeBits = 12,
                MemorySizeBits = 13
            };

            // Create store
            _kvStore = new(1L << 20, logSettings);
            _clientSessionBuilder = _kvStore.For(_simpleFunctions);
        }

        public void Upsert(int key, int value)
        {
            var session = GetPooledSession();
            session.Upsert(key, value);
            _sessionPool.Enqueue(session);
        }

        public async Task<(Status, int?)> ReadAsync(int key)
        {
            var session = GetPooledSession();
            (Status, int) result = (await session.ReadAsync(key).ConfigureAwait(false)).Complete();
            _sessionPool.Enqueue(session);

            return result;
        }

        private ClientSession<int, int, int, int, Empty, SimpleFunctions<int, int, Empty>> GetPooledSession()
        {
            if (_sessionPool.TryDequeue(out ClientSession<int, int, int, int, Empty, SimpleFunctions<int, int, Empty>>? result))
            {
                return result;
            }

            return _clientSessionBuilder.NewSession<SimpleFunctions<int, int, Empty>>();
        }

        public void Dispose()
        {
            foreach (var session in _sessionPool)
            {
                session.Dispose();
            }

            _kvStore.Dispose();
        }
    }
}
