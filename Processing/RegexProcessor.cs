using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using VKR_MPI_V1.File_Processing;

namespace VKR_MPI_V1.Processing
{
    public class RegexProcessor
    {
        private readonly Regex _regex;

        public RegexProcessor(string pattern)
        {
            _regex = new Regex(pattern, RegexOptions.Compiled | RegexOptions.Multiline);
        }

        public long ProcessChunk(Chunk chunk)
        {
            long matches = 0;

            string text = Encoding.UTF8.GetString(chunk.Buffer, 0, chunk.Length);

            foreach (Match m in _regex.Matches(text))
            {
                long matchPos = chunk.GlobalStart + m.Index;

                // игнорируем overlap
                if (matchPos >= chunk.ValidEnd)
                    continue;

                matches++;
            }
            Console.WriteLine($"Matches in chunk starting {chunk.GlobalStart}: {matches}");
            return matches;
        }
    }
}
