namespace VKR_MPI_V1;

[Serializable]
public sealed class AppConfig
{
    public string FilePath { get; set; } = string.Empty;

    public bool EnableAvx2Prefilter { get; set; } = false;

    public string[] Avx2PrefilterRequiredGroups { get; set; } = Array.Empty<string>();
    public string OutputPath { get; set; } = "result.txt";
    public string Pattern { get; set; } = string.Empty;
    public int ChunkSizeMB { get; set; } = 16;
    public int OverlapKB { get; set; } = 64;
    public int WorkerThreads { get; set; } = Environment.ProcessorCount;
    public int WorkerSubChunkOverlapChars { get; set; } = 1024;
    public string EncodingName { get; set; } = "utf-8";
    public bool Recursive { get; set; } = true;
    public int RegexTimeoutMs { get; set; } = 0;

    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(FilePath))
            throw new InvalidOperationException("FilePath is empty.");
        if (string.IsNullOrWhiteSpace(Pattern))
            throw new InvalidOperationException("Pattern is empty.");
        if (ChunkSizeMB <= 0)
            throw new InvalidOperationException("ChunkSizeMB must be > 0.");
        if (OverlapKB < 0)
            throw new InvalidOperationException("OverlapKB must be >= 0.");
        if (WorkerThreads <= 0)
            throw new InvalidOperationException("WorkerThreads must be > 0.");
        if (WorkerSubChunkOverlapChars < 0)
            throw new InvalidOperationException("WorkerSubChunkOverlapChars must be >= 0.");
        if (RegexTimeoutMs < 0)
            throw new InvalidOperationException("RegexTimeoutMs must be >= 0.");
    }

    public int ChunkSizeBytes => checked(ChunkSizeMB * 1024 * 1024);
    public int OverlapBytes => checked(OverlapKB * 1024);
}