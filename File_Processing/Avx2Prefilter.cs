using System;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

public static class Avx2Prefilter
{
    private const int VectorSize = 32;

    public static bool ContainsAllGroups(ReadOnlySpan<byte> data, byte[][] requiredGroups)
    {
        if (requiredGroups == null || requiredGroups.Length == 0)
            return true;

        foreach (byte[] group in requiredGroups)
        {
            if (group == null || group.Length == 0)
                continue;

            if (!ContainsAny(data, group))
                return false;
        }

        return true;
    }

    public static bool ContainsAny(ReadOnlySpan<byte> data, ReadOnlySpan<byte> needles)
    {
        if (data.IsEmpty || needles.IsEmpty)
            return false;

        if (Avx2.IsSupported && data.Length >= VectorSize)
            return ContainsAnyAvx2(data, needles);

        return ContainsAnyScalar(data, needles);
    }

    private static bool ContainsAnyAvx2(ReadOnlySpan<byte> data, ReadOnlySpan<byte> needles)
    {
        int i = 0;
        int lastVectorStart = data.Length - VectorSize;

        for (; i <= lastVectorStart; i += VectorSize)
        {
            Vector256<byte> chunk = MemoryMarshal.Read<Vector256<byte>>(
                data.Slice(i, VectorSize));

            Vector256<byte> matches = Vector256<byte>.Zero;

            for (int n = 0; n < needles.Length; n++)
            {
                Vector256<byte> needle = Vector256.Create(needles[n]);
                Vector256<byte> comparison = Avx2.CompareEqual(chunk, needle);

                matches = Avx2.Or(matches, comparison);
            }

            if (Avx2.MoveMask(matches) != 0)
                return true;
        }

        if (i < data.Length)
            return ContainsAnyScalar(data.Slice(i), needles);

        return false;
    }

    private static bool ContainsAnyScalar(ReadOnlySpan<byte> data, ReadOnlySpan<byte> needles)
    {
        for (int i = 0; i < data.Length; i++)
        {
            byte current = data[i];

            for (int n = 0; n < needles.Length; n++)
            {
                if (current == needles[n])
                    return true;
            }
        }

        return false;
    }

    public static bool ContainsAllWordGroups(
        ReadOnlySpan<byte> data,
        byte[][][] requiredWordGroups)
    {
        if (requiredWordGroups == null || requiredWordGroups.Length == 0)
            return true;

        foreach (byte[][] group in requiredWordGroups)
        {
            if (group == null || group.Length == 0)
                continue;

            if (!ContainsAnyWord(data, group))
                return false;
        }

        return true;
    }

    public static bool ContainsAnyWord(
        ReadOnlySpan<byte> data,
        byte[][] words)
    {
        if (data.IsEmpty || words == null || words.Length == 0)
            return false;

        foreach (byte[] word in words)
        {
            if (word == null || word.Length == 0)
                continue;

            if (data.IndexOf(word) >= 0)
                return true;
        }

        return false;
    }
}