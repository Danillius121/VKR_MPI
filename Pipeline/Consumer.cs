using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using VKR_MPI_V1.File_Processing;
using VKR_MPI_V1.Processing;

namespace VKR_MPI_V1.Pipeline
{
    public static class Consumer
    {
        // ИСПРАВЛЕНИЕ: Добавили аргумент filePath
        public static long ProcessFile(string filePath, AppConfig config)
        {
            int chunkSize = config.ChunkSizeMB * 1024 * 1024;
            int overlap = config.OverlapKB * 1024;

            var reader = new Chunk_Reader(filePath, chunkSize, overlap);
            var processor = new RegexProcessor(config.Pattern);

            var queue = new BlockingCollection<Chunk>(boundedCapacity: 8);

            long total = 0;

            var readerThread = new Thread(() =>
            {
                foreach (var chunk in reader.ReadChunks())
                    queue.Add(chunk);

                queue.CompleteAdding();
            });

            readerThread.Start();

            Parallel.For(0, config.WorkerThreads, i =>
            {
                foreach (var chunk in queue.GetConsumingEnumerable())
                {
                    long local = processor.ProcessChunk(chunk);
                    Interlocked.Add(ref total, local);
                }
            });

            readerThread.Join();

            return total;
        }
    }
}
