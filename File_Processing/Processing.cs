using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.Text;
using System.Text.RegularExpressions;
using System.Text;
using System.Runtime.Intrinsics.X86;

namespace VKR_MPI_V1
{
    public interface IChunkProcessor
    {
        long Process(Chunk chunk);
    }

    public interface IChunkProcessorFactory
    {
        IChunkProcessor Create(AppConfig config);
    }

    public sealed class ChunkProcessorFactory : IChunkProcessorFactory
    {
        public IChunkProcessor Create(AppConfig config) => new RegexChunkProcessor(config);
    }

    public sealed class RegexChunkProcessor : IChunkProcessor
    {
        private readonly byte[][] _avx2PrefilterRequiredGroups;
        private readonly bool _enableAvx2Prefilter;
        private readonly Regex _regex;
        private readonly Encoding _encoding;
        private readonly int _workerThreads;
        private readonly int _intraChunkOverlapChars;

        public RegexChunkProcessor(AppConfig config)
        {
            _enableAvx2Prefilter = config.EnableAvx2Prefilter;

            _avx2PrefilterRequiredGroups = config.Avx2PrefilterRequiredGroups
                .Where(group => !string.IsNullOrWhiteSpace(group))
                .Select(group => Encoding.ASCII.GetBytes(group))
                .ToArray();
            _encoding = Encoding.GetEncoding(config.EncodingName);
            _workerThreads = Math.Max(1, config.WorkerThreads);
            _intraChunkOverlapChars = Math.Max(0, config.WorkerSubChunkOverlapChars);

            var options = RegexOptions.Compiled
                        | RegexOptions.Multiline
                        | RegexOptions.CultureInvariant;

            var timeout = config.RegexTimeoutMs > 0
                ? TimeSpan.FromMilliseconds(config.RegexTimeoutMs)
                : Regex.InfiniteMatchTimeout;

            _regex = new Regex(config.Pattern, options, timeout);
        }

        public long Process(Chunk chunk)
        {
            if (chunk.Buffer.Length == 0 || chunk.ReadLength == 0)
                return 0;
            if (_enableAvx2Prefilter && _avx2PrefilterRequiredGroups.Length > 0)
            {
                ReadOnlySpan<byte> data = chunk.Buffer.AsSpan(0, chunk.ReadLength);

                if (!Avx2Prefilter.ContainsAllGroups(data, _avx2PrefilterRequiredGroups))
                    return 0;
            }
            // Полный текст чанка, включая overlap в конце.
            string fullText = _encoding.GetString(chunk.Buffer, 0, chunk.ReadLength);

            // Граница основной части чанка (без suffix overlap) в char-координатах.
            int primaryCharLimit = _encoding.GetCharCount(chunk.Buffer, 0, chunk.PrimaryLength);

            var slices = BuildSlices(fullText, _workerThreads, _intraChunkOverlapChars);

            long total = 0;
            var options = new ParallelOptions { MaxDegreeOfParallelism = _workerThreads };

            Parallel.ForEach(slices, options, slice =>
            {
                long local = 0;

                string segment = fullText.Substring(slice.Start, slice.ReadLength);

                foreach (Match match in _regex.Matches(segment))
                {
                    
                    int globalMatchStart = slice.Start + match.Index;

                    // Считаем только совпадения, которые стартуют в основной части чанка.
                    if (globalMatchStart < primaryCharLimit)
                        local++;
                }

                if (local != 0)
                    Interlocked.Add(ref total, local);
            });

            return total;
        }

        private static List<TextSlice> BuildSlices(string text, int desiredSlices, int overlapChars)
        {
            var result = new List<TextSlice>();

            if (text.Length == 0)
                return result;

            desiredSlices = Math.Clamp(desiredSlices, 1, Math.Max(1, text.Length));
            int approxSize = Math.Max(1, text.Length / desiredSlices);

            int start = 0;
            for (int i = 0; i < desiredSlices && start < text.Length; i++)
            {
                int primaryEnd = (i == desiredSlices - 1)
                    ? text.Length
                    : Math.Min(text.Length, start + approxSize);

                if (primaryEnd < text.Length)
                {
                    while (primaryEnd < text.Length && text[primaryEnd] != '\n')
                        primaryEnd++;

                    if (primaryEnd < text.Length)
                        primaryEnd++; // включаем '\n'
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
