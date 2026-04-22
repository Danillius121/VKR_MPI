using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace VKR_MPI_V1
{
    [Serializable]
    public sealed class Chunk
    {
        public string FilePath { get; set; } = string.Empty;

        public long FileIndex { get; set; }

        public long ChunkIndex { get; set; }

        public long GlobalByteOffset { get; set; }

        // Число байт основной области чанка без suffix overlap.
        public int PrimaryLength { get; set; }

        // Реально прочитанные байты, включая overlap в конце.
        public int ReadLength { get; set; }

        public bool IsStopSignal { get; set; }

        public byte[] Buffer { get; set; } = Array.Empty<byte>();

        public static Chunk StopSignal() => new Chunk { IsStopSignal = true };
    }

    [Serializable]
    public sealed class ChunkResult
    {
        public string FilePath { get; set; } = string.Empty;

        public long FileIndex { get; set; }

        public long ChunkIndex { get; set; }

        public int WorkerRank { get; set; }

        public long Matches { get; set; }

        public long BytesProcessed { get; set; }

        public string? Error { get; set; }
    }
}