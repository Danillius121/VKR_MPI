using MPI;
using System.Text;
using System.Text.RegularExpressions;
using System.Diagnostics;

namespace VKR_MPI_V1;

internal static class WorkerNode
{

    public static void Run(Communicator nodeComm, AppConfig config)
    {
        var processor = new RegexChunkProcessor(config);

        while (true)
        {
            (ChunkHeader header, byte[] buffer) = MpiWire.ReceiveChunk(nodeComm, 0);

            if (header.IsStopSignal)
                return;

            try
            {
                long matches = processor.Process(buffer, header.PrimaryLength, header.ReadLength);

                var result = new ResultHeader(
                    WorkerRank: nodeComm.Rank,
                    FileIndex: header.FileIndex,
                    ChunkIndex: header.ChunkIndex,
                    Success: true,
                    Matches: matches,
                    BytesProcessed: header.PrimaryLength,
                    ErrorMessage: null);

                MpiWire.SendResult(nodeComm, 0, result);
            }
            catch (Exception ex)
            {
                var result = new ResultHeader(
                    WorkerRank: nodeComm.Rank,
                    FileIndex: header.FileIndex,
                    ChunkIndex: header.ChunkIndex,
                    Success: false,
                    Matches: 0,
                    BytesProcessed: 0,
                    ErrorMessage: ex.Message);

                MpiWire.SendResult(nodeComm, 0, result);
            }
        }
    }

    private sealed class RegexChunkProcessor
    {
        private readonly byte[][] _avx2PrefilterRequiredGroups;
        private readonly bool _enableAvx2Prefilter;
        private readonly byte[][][] _avx2PrefilterRequiredWordGroups;
        private readonly Regex _regex;
        private readonly Encoding _encoding;
        private readonly int _workerThreads;
        private readonly int _sliceOverlapChars;
        private long _prefilterChecked;
        private long _prefilterRejected;
        private long _prefilterAccepted;
        private long _wordPrefilterChecked;
        private long _wordPrefilterRejected;
        private long _wordPrefilterAccepted;

        public RegexChunkProcessor(AppConfig config)
        {
            
            _enableAvx2Prefilter = config.EnableAvx2Prefilter;

            _avx2PrefilterRequiredGroups = config.Avx2PrefilterRequiredGroups
                .Where(group => !string.IsNullOrWhiteSpace(group))
                .Select(group => Encoding.ASCII.GetBytes(group))
                .ToArray();
            _avx2PrefilterRequiredWordGroups = config.Avx2PrefilterRequiredWordGroups
            .Where(group => group != null && group.Length > 0)
            .Select(group => group
                .Where(word => !string.IsNullOrWhiteSpace(word))
                .Select(word => Encoding.ASCII.GetBytes(word))
                .ToArray())
            .Where(group => group.Length > 0)
            .ToArray();
            _encoding = Encoding.GetEncoding(config.EncodingName);
            _workerThreads = Math.Max(1, config.WorkerThreads);
            _sliceOverlapChars = Math.Max(0, config.WorkerSubChunkOverlapChars);

            var options = RegexOptions.Compiled | RegexOptions.Multiline | RegexOptions.CultureInvariant;
            var timeout = config.RegexTimeoutMs > 0
                ? TimeSpan.FromMilliseconds(config.RegexTimeoutMs)
                : Regex.InfiniteMatchTimeout;

            _regex = new Regex(config.Pattern, options, timeout);
        }

        public long Process(byte[] buffer, int primaryLength, int readLength)
        {
            Stopwatch totalSw = Stopwatch.StartNew();

            long prefilterMs = 0;
            long decodeMs = 0;
            long regexMs = 0;

            bool rejectedByPrefilter = false;

            if (_enableAvx2Prefilter && _avx2PrefilterRequiredWordGroups.Length > 0)
            {
                Stopwatch prefilterSw = Stopwatch.StartNew();

                ReadOnlySpan<byte> data = buffer.AsSpan(0, readLength);

                if (!Avx2Prefilter.ContainsAllWordGroups(data, _avx2PrefilterRequiredWordGroups))
                {
                    rejectedByPrefilter = true;
                }

                prefilterSw.Stop();
                prefilterMs = prefilterSw.ElapsedMilliseconds;

                if (rejectedByPrefilter)
                {
                    totalSw.Stop();

                    Logger.Info(
                        $"Chunk timing: total={totalSw.ElapsedMilliseconds} ms, " +
                        $"prefilter={prefilterMs} ms, " +
                        $"decode=0 ms, regex=0 ms, " +
                        $"readLength={readLength}, rejectedByPrefilter=True");

                    return 0;
                }
            }

            Stopwatch decodeSw = Stopwatch.StartNew();

            string text = _encoding.GetString(buffer, 0, readLength);

            decodeSw.Stop();
            decodeMs = decodeSw.ElapsedMilliseconds;

            Stopwatch regexSw = Stopwatch.StartNew();

            int primaryCharLimit = _encoding.GetCharCount(buffer, 0, primaryLength);

            var slices = BuildSlices(text, _workerThreads, _sliceOverlapChars);
            long total = 0;
            
            Parallel.ForEach(
                slices,
                new ParallelOptions { MaxDegreeOfParallelism = _workerThreads },
                slice =>
                {
                    long local = 0;
                    string segment = text.Substring(slice.Start, slice.ReadLength);

                    foreach (Match match in _regex.Matches(segment))
                    {
                        //Console.WriteLine(segment, "\n");
                        int globalMatchStart = slice.Start + match.Index;
                        if (globalMatchStart < primaryCharLimit)
                            local++;
                    }

                    if (local != 0)
                        Interlocked.Add(ref total, local);
                });
            regexSw.Stop();
            regexMs = regexSw.ElapsedMilliseconds;

            totalSw.Stop();

            Logger.Info(
                $"Chunk timing: total={totalSw.ElapsedMilliseconds} ms, " +
                $"prefilter={prefilterMs} ms, " +
                $"decode={decodeMs} ms, " +
                $"regex={regexMs} ms, " +
                $"readLength={readLength}, matches={total}, " +
                $"rejectedByPrefilter=False");

            

            return total;
        }

        private static List<TextSlice> BuildSlices(string text, int desiredSlices, int overlapChars)
        {
            var result = new List<TextSlice>();
            if (text.Length == 0)
                return result;

            desiredSlices = Math.Clamp(desiredSlices, 1, Math.Max(1, text.Length));
            int approx = Math.Max(1, text.Length / desiredSlices);

            int start = 0;
            for (int i = 0; i < desiredSlices && start < text.Length; i++)
            {
                int primaryEnd = (i == desiredSlices - 1)
                    ? text.Length
                    : Math.Min(text.Length, start + approx);

                if (primaryEnd < text.Length)
                {
                    while (primaryEnd < text.Length && text[primaryEnd] != '\n')
                        primaryEnd++;

                    if (primaryEnd < text.Length)
                        primaryEnd++;
                }

                int readEnd = Math.Min(text.Length, primaryEnd + overlapChars);
                result.Add(new TextSlice(start, primaryEnd, readEnd - start));
                start = primaryEnd;
            }

            if (result.Count == 0)
                result.Add(new TextSlice(0, text.Length, text.Length));

            return result;
        }

        private readonly record struct TextSlice(int Start, int PrimaryEndExclusive, int ReadLength);
    }
}