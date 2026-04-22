using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace VKR_MPI_V1
{
    internal static class InputDiscovery
    {
        public static IReadOnlyList<string> ExpandInputs(string inputPath, bool recursive)
        {
            inputPath = Path.IsPathRooted(inputPath)
                ? inputPath
                : Path.GetFullPath(inputPath);

            if (File.Exists(inputPath))
                return new[] { inputPath };

            if (!Directory.Exists(inputPath))
                throw new DirectoryNotFoundException($"Input path not found: {inputPath}");

            var search = recursive ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly;

            return Directory.EnumerateFiles(inputPath, "*", search)
                .OrderBy(p => p, StringComparer.OrdinalIgnoreCase)
                .ToArray();
        }
    }

    internal sealed class FileChunkScheduler : IDisposable
    {
        private readonly IReadOnlyList<string> _files;
        private readonly AppConfig _config;
        private readonly Encoding _encoding;

        private int _fileIndex;
        private long _chunkIndexInFile;
        private FileChunkReader? _reader;

        public FileChunkScheduler(IReadOnlyList<string> files, AppConfig config)
        {
            _files = files;
            _config = config;
            _encoding = Encoding.GetEncoding(config.EncodingName);
        }

        public bool TryGetNextChunk(out Chunk? chunk)
        {
            while (true)
            {
                if (_reader is null)
                {
                    if (_fileIndex >= _files.Count)
                    {
                        chunk = null;
                        return false;
                    }

                    string filePath = _files[_fileIndex];
                    _reader = new FileChunkReader(filePath, _config.ChunkSizeBytes, _config.OverlapBytes, _encoding);
                    _chunkIndexInFile = 0;
                }

                if (_reader.TryReadNext(out chunk))
                {
                    chunk.FileIndex = _fileIndex;
                    chunk.ChunkIndex = _chunkIndexInFile++;
                    return true;
                }

                _reader.Dispose();
                _reader = null;
                _fileIndex++;
            }
        }

        public void Dispose()
        {
            _reader?.Dispose();
            _reader = null;
        }
    }

    internal sealed class FileChunkReader : IDisposable
    {
        private readonly FileStream _stream;
        private readonly int _chunkSizeBytes;
        private readonly int _overlapBytes;
        private readonly Encoding _encoding;
        private readonly string _filePath;

        private long _offset;

        public FileChunkReader(string filePath, int chunkSizeBytes, int overlapBytes, Encoding encoding)
        {
            _filePath = filePath;
            _chunkSizeBytes = chunkSizeBytes;
            _overlapBytes = overlapBytes;
            _encoding = encoding;

            _stream = new FileStream(
                filePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                bufferSize: 4 * 1024 * 1024,
                FileOptions.SequentialScan);
        }

        public bool TryReadNext(out Chunk? chunk)
        {
            chunk = null;

            if (_offset >= _stream.Length)
                return false;

            long remaining = _stream.Length - _offset;

            int primaryLength = (int)Math.Min(_chunkSizeBytes, remaining);
            int readLength = (int)Math.Min((long)primaryLength + _overlapBytes, remaining);

            if (readLength <= 0)
                return false;

            byte[] buffer = new byte[readLength];

            _stream.Seek(_offset, SeekOrigin.Begin);

            int totalRead = 0;
            while (totalRead < readLength)
            {
                int read = _stream.Read(buffer, totalRead, readLength - totalRead);
                if (read == 0)
                    break;

                totalRead += read;
            }

            if (totalRead == 0)
                return false;

            if (totalRead != buffer.Length)
                Array.Resize(ref buffer, totalRead);

            chunk = new Chunk
            {
                FilePath = _filePath,
                GlobalByteOffset = _offset,
                PrimaryLength = Math.Min(primaryLength, totalRead),
                ReadLength = totalRead,
                Buffer = buffer
            };

            _offset += primaryLength;

            return true;
        }

        public void Dispose()
        {
            _stream.Dispose();
        }
    }
}
