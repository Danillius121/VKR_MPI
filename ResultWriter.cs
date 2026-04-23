using System.Text;

namespace VKR_MPI_V1;

internal sealed class ResultWriter : IDisposable
{
    private readonly StreamWriter _writer;
    private readonly StringBuilder _buffer = new();
    private readonly int _flushThreshold;

    public ResultWriter(int rank, int nodeColor, int flushThresholdBytes = 1_000_000)
    {
        Directory.CreateDirectory("results");

        string fileName = $"results/node{nodeColor}_rank{rank}.log";
        _writer = new StreamWriter(fileName, append: false, Encoding.UTF8);

        _flushThreshold = flushThresholdBytes;
    }

    public void Write(long fileIndex, long globalOffset, string match)
    {
        _buffer.Append(fileIndex)
               .Append('|')
               .Append(globalOffset)
               .Append('|')
               .AppendLine(match);

        if (_buffer.Length >= _flushThreshold)
            Flush();
    }

    public void Flush()
    {
        _writer.Write(_buffer.ToString());
        _buffer.Clear();
        _writer.Flush();
    }

    public void Dispose()
    {
        Flush();
        _writer.Dispose();
    }
}