using System.Text;
using MPI;

namespace VKR_MPI_V1;

internal static class MpiTags
{
    public const int ChunkHeaderLength = 1001;
    public const int ChunkHeaderData = 1002;

    public const int ChunkBufferLength = 1011;
    public const int ChunkBufferData = 1012;

    public const int ResultLength = 2001;
    public const int ResultData = 2002;
}

internal readonly record struct ChunkHeader(
    bool IsStopSignal,
    long FileIndex,
    long ChunkIndex,
    long GlobalByteOffset,
    int PrimaryLength,
    int ReadLength,
    int BufferLength);

internal readonly record struct ResultHeader(
    int WorkerRank,
    long FileIndex,
    long ChunkIndex,
    bool Success,
    long Matches,
    long BytesProcessed,
    string? ErrorMessage);

internal static class MpiWire
{
    public static byte[] PackChunkHeader(in ChunkHeader h)
    {
        using var ms = new MemoryStream(64);
        using var bw = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true);

        bw.Write(0x51484D50); // QHMP
        bw.Write(1);          // version
        bw.Write(h.IsStopSignal);
        bw.Write(h.FileIndex);
        bw.Write(h.ChunkIndex);
        bw.Write(h.GlobalByteOffset);
        bw.Write(h.PrimaryLength);
        bw.Write(h.ReadLength);
        bw.Write(h.BufferLength);

        bw.Flush();
        return ms.ToArray();
    }

    public static ChunkHeader UnpackChunkHeader(byte[] data)
    {
        using var ms = new MemoryStream(data, writable: false);
        using var br = new BinaryReader(ms, Encoding.UTF8, leaveOpen: true);

        int magic = br.ReadInt32();
        int version = br.ReadInt32();

        if (magic != 0x51484D50 || version != 1)
            throw new InvalidDataException("Invalid chunk header.");

        return new ChunkHeader(
            br.ReadBoolean(),
            br.ReadInt64(),
            br.ReadInt64(),
            br.ReadInt64(),
            br.ReadInt32(),
            br.ReadInt32(),
            br.ReadInt32());
    }

    public static byte[] PackResultHeader(in ResultHeader r)
    {
        using var ms = new MemoryStream(96);
        using var bw = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true);

        bw.Write(0x52534C54); // RSLT
        bw.Write(1);          // version
        bw.Write(r.WorkerRank);
        bw.Write(r.FileIndex);
        bw.Write(r.ChunkIndex);
        bw.Write(r.Success);
        bw.Write(r.Matches);
        bw.Write(r.BytesProcessed);
        bw.Write(r.ErrorMessage ?? string.Empty);

        bw.Flush();
        return ms.ToArray();
    }

    public static ResultHeader UnpackResultHeader(byte[] data)
    {
        using var ms = new MemoryStream(data, writable: false);
        using var br = new BinaryReader(ms, Encoding.UTF8, leaveOpen: true);

        int magic = br.ReadInt32();
        int version = br.ReadInt32();

        if (magic != 0x52534C54 || version != 1)
            throw new InvalidDataException("Invalid result header.");

        return new ResultHeader(
            br.ReadInt32(),
            br.ReadInt64(),
            br.ReadInt64(),
            br.ReadBoolean(),
            br.ReadInt64(),
            br.ReadInt64(),
            br.ReadString());
    }

    public static void SendBytes(Communicator comm, int dest, int lenTag, int dataTag, byte[] data)
    {
        comm.Send(data.Length, dest, lenTag);
        comm.Send(data, dest, dataTag);
    }

    public static byte[] ReceiveBytes(Communicator comm, int source, int lenTag, int dataTag, out int actualSource)
    {
        comm.Receive(source, lenTag, out int len, out CompletedStatus status);
        actualSource = status.Source;

        if (len < 0)
            throw new InvalidDataException($"Negative payload length received: {len}");

        byte[] buffer = new byte[len];
        comm.Receive(actualSource, dataTag, ref buffer);
        return buffer;
    }

    public static void SendChunk(Communicator comm, int dest, in ChunkHeader header, byte[] buffer)
    {
        byte[] headerBytes = PackChunkHeader(header);
        SendBytes(comm, dest, MpiTags.ChunkHeaderLength, MpiTags.ChunkHeaderData, headerBytes);
        SendBytes(comm, dest, MpiTags.ChunkBufferLength, MpiTags.ChunkBufferData, buffer);
    }

    public static (ChunkHeader Header, byte[] Buffer) ReceiveChunk(Communicator comm, int source)
    {
        int src1 = source;
        byte[] headerBytes = ReceiveBytes(comm, src1, MpiTags.ChunkHeaderLength, MpiTags.ChunkHeaderData, out _);
        ChunkHeader header = UnpackChunkHeader(headerBytes);

        if (header.IsStopSignal)
            return (header, Array.Empty<byte>());

        byte[] buffer = ReceiveBytes(comm, source, MpiTags.ChunkBufferLength, MpiTags.ChunkBufferData, out _);
        return (header, buffer);
    }

    public static void SendResult(Communicator comm, int dest, in ResultHeader result)
    {
        byte[] resultBytes = PackResultHeader(result);
        SendBytes(comm, dest, MpiTags.ResultLength, MpiTags.ResultData, resultBytes);
    }

    public static ResultHeader ReceiveResult(Communicator comm, out int source)
    {
        byte[] bytes = ReceiveBytes(comm, Communicator.anySource, MpiTags.ResultLength, MpiTags.ResultData, out source);
        return UnpackResultHeader(bytes);
    }
}