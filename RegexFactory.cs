using System;
using System.Text.RegularExpressions;

namespace VKR_MPI_V1
{

    public static class RegexFactory
    {
        public static Regex Create(AppConfig config)
        {
            RegexOptions options =
                RegexOptions.Compiled |
                RegexOptions.Multiline |
                RegexOptions.CultureInvariant;

            TimeSpan timeout = TimeSpan.FromSeconds(10);

            return new Regex(config.Pattern, options, timeout);
        }
    }
}
