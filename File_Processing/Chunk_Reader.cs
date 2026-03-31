using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace VKR_MPI_V1.File_Processing
{
    public class Chunk_Reader
    {
        private readonly FileStream _fs;
        private readonly int _chunkSize;
        private readonly int _overlap;

        public Chunk_Reader(string path, int chunkSize, int overlap)
        {
            _fs = new FileStream(
                path,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                bufferSize: 4 * 1024 * 1024,
                FileOptions.SequentialScan);

            _chunkSize = chunkSize;
            _overlap = overlap;
        }

        public IEnumerable<Chunk> ReadChunks()
        {
            long offset = 0;
            byte[] buffer = new byte[_chunkSize + _overlap];

            while (true)
            {
                _fs.Seek(offset, SeekOrigin.Begin);

                int read = _fs.Read(buffer, 0, buffer.Length);
                if (read == 0)
                    yield break;

                yield return new Chunk
                {
                    Buffer = buffer,
                    Length = read,
                    GlobalStart = offset,
                    ValidEnd = offset + _chunkSize
                };

                offset += _chunkSize;

                if (offset >= _fs.Length)
                    yield break;
            }
        }
    }
}
