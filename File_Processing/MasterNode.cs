using MPI;
using System.Diagnostics;
using System.Text;

namespace VKR_MPI_V1;

internal static class MasterNode
{

    public static long Run(Communicator nodeComm, AppConfig config)
    {
        var files = InputDiscovery.ExpandInputs(config.FilePath, config.Recursive);

        if (nodeComm.Size < 2)
        {
            Logger.Warn($"Node {System.Environment.MachineName}: only one rank here, master will read and count locally.");
            // Можно либо считать полностью на этом rank, либо оставить без worker'ов.
            // Проще: работаем как single-process master.
        }

        Logger.Info($"Node {System.Environment.MachineName} | local ranks: {nodeComm.Size}");
        Logger.Info($"Local files: {files.Count}");

        using var scheduler = new FileChunkScheduler(files, config);

        long totalMatches = 0;
        int inFlight = 0;

        // Раздаём чанки только локальным worker'ам.
        for (int worker = 1; worker < nodeComm.Size; worker++)
        {
            if (!scheduler.TryGetNextChunk(out var job))
                break;

            MpiWire.SendChunk(nodeComm, worker, job.Header, job.Buffer);
            inFlight++;
        }

        while (inFlight > 0)
        {
            ResultHeader result = MpiWire.ReceiveResult(nodeComm, out int source);
            inFlight--;

            if (result.Success)
                totalMatches += result.Matches;
            else
                Logger.Error($"Local worker {result.WorkerRank}: {result.ErrorMessage}");

            if (scheduler.TryGetNextChunk(out var nextJob))
            {
                MpiWire.SendChunk(nodeComm, source, nextJob.Header, nextJob.Buffer);
                inFlight++;
            }
        }

        StopAllWorkers(nodeComm);

        Logger.Info($"Node {System.Environment.MachineName}: local matches = {totalMatches}");
        return totalMatches;
    }



    private static void StopAllWorkers(Communicator nodeComm)
    {
        var stopHeader = new ChunkHeader(
            IsStopSignal: true,
            FileIndex: -1,
            ChunkIndex: -1,
            GlobalByteOffset: 0,
            PrimaryLength: 0,
            ReadLength: 0,
            BufferLength: 0);

        for (int worker = 1; worker < nodeComm.Size; worker++)
            MpiWire.SendChunk(nodeComm, worker, stopHeader, Array.Empty<byte>());
    }

    private sealed record ChunkJob(ChunkHeader Header, byte[] Buffer);

    private sealed class FileChunkScheduler : IDisposable
    {
        private readonly IReadOnlyList<string> _files;
        private readonly AppConfig _config;
        private int _fileIndex;
        private long _chunkIndexInFile;
        private FileChunkReader? _reader;

        public FileChunkScheduler(IReadOnlyList<string> files, AppConfig config)
        {
            _files = files;
            _config = config;
        }

        public bool TryGetNextChunk(out ChunkJob job)
        {
            while (true)
            {
                if (_reader is null)
                {
                    if (_fileIndex >= _files.Count)
                    {
                        job = default!;
                        return false;
                    }

                    _reader = new FileChunkReader(_files[_fileIndex], _config.ChunkSizeBytes, _config.OverlapBytes);
                    _chunkIndexInFile = 0;
                }

                if (_reader.TryReadNext(out var buffer, out int primaryLength, out int readLength, out long offset))
                {
                    var header = new ChunkHeader(
                        IsStopSignal: false,
                        FileIndex: _fileIndex,
                        ChunkIndex: _chunkIndexInFile++,
                        GlobalByteOffset: offset,
                        PrimaryLength: primaryLength,
                        ReadLength: readLength,
                        BufferLength: buffer.Length);

                    job = new ChunkJob(header, buffer);
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

    private sealed class FileChunkReader : IDisposable
    {
        private readonly FileStream _stream;
        private readonly int _chunkSizeBytes;
        private readonly int _overlapBytes;
        private long _offset;

        public FileChunkReader(string filePath, int chunkSizeBytes, int overlapBytes)
        {
            _chunkSizeBytes = chunkSizeBytes;
            _overlapBytes = overlapBytes;

            _stream = new FileStream(
                filePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                bufferSize: 4 * 1024 * 1024,
                FileOptions.SequentialScan);
        }

        public bool TryReadNext(out byte[] buffer, out int primaryLength, out int readLength, out long offset)
        {
            buffer = Array.Empty<byte>();
            primaryLength = 0;
            readLength = 0;
            offset = _offset;

            if (_offset >= _stream.Length)
                return false;

            long remaining = _stream.Length - _offset;
            primaryLength = (int)Math.Min(_chunkSizeBytes, remaining);
            readLength = (int)Math.Min((long)primaryLength + _overlapBytes, remaining);

            buffer = new byte[readLength];

            _stream.Seek(_offset, SeekOrigin.Begin);

            int total = 0;
            while (total < readLength)
            {
                int read = _stream.Read(buffer, total, readLength - total);
                if (read == 0)
                    break;

                total += read;
            }

            if (total == 0)
                return false;

            if (total != buffer.Length)
                Array.Resize(ref buffer, total);

            readLength = buffer.Length;
            primaryLength = Math.Min(primaryLength, readLength);

            _offset += primaryLength;
            return true;
        }

        public void Dispose() => _stream.Dispose();
    }

    private static class InputDiscovery
    {
        public static IReadOnlyList<string> ExpandInputs(string inputPath, bool recursive)
        {
            inputPath = Path.IsPathRooted(inputPath) ? inputPath : Path.GetFullPath(inputPath);

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
}