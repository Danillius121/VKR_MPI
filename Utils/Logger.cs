namespace VKR_MPI_V1;

internal static class Logger
{
    private static readonly object Sync = new();

    public static void Info(string message) => Write("INFO", message);
    public static void Warn(string message) => Write("WARN", message);
    public static void Error(string message) => Write("ERROR", message);

    public static string FormatRate(long bytes, TimeSpan elapsed)
    {
        if (elapsed.TotalSeconds <= 0)
            return "n/a";

        double mb = bytes / 1024d / 1024d;
        double rate = mb / elapsed.TotalSeconds;
        return $"{rate:F2} MB/s";
    }

    private static void Write(string level, string message)
    {
        lock (Sync)
        {
            Console.WriteLine($"[{DateTime.Now:HH:mm:ss}][{level}] {message}");
        }
    }
}