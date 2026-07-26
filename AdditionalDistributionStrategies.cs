using MPI;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;


namespace VKR_MPI_V1
{

    public static class AdditionalDistributionStrategies
    {
        private enum MsgTag
        {
            StaticFilePathLength = 310,
            StaticFilePathData = 311,

            DynamicTaskRequest = 320,
            DynamicTaskResponse = 321,
            DynamicResultReport = 322
        }

        public static long RunStaticFileDistribution(
            Communicator comm,
            string targetPath,
            Regex regex,
            bool saveToFile = false)
        {
            List<string> myTaskFiles = new();

            if (comm.Rank == 0)
            {
                Logger.Info("[MASTER] StaticFiles: scanning directory...");

                List<string> allFiles = GetFilesSafe(targetPath);

                Logger.Info($"[MASTER] StaticFiles: found files = {allFiles.Count}");

                for (int i = 0; i < allFiles.Count; i++)
                {
                    int targetRank = i % comm.Size;

                    if (targetRank == 0)
                    {
                        myTaskFiles.Add(allFiles[i]);
                    }
                    else
                    {
                        SendString(comm, targetRank, allFiles[i]);
                    }
                }

                for (int rank = 1; rank < comm.Size; rank++)
                {
                    int stopSignal = -1;
                    comm.Send(stopSignal, rank, (int)MsgTag.StaticFilePathLength);
                }
            }
            else
            {
                while (true)
                {
                    int length = 0;
                    comm.Receive(0, (int)MsgTag.StaticFilePathLength, out length);

                    if (length == -1)
                        break;

                    byte[] buffer = new byte[length];
                    comm.Receive(0, (int)MsgTag.StaticFilePathData, ref buffer);

                    string filePath = Encoding.UTF8.GetString(buffer);
                    myTaskFiles.Add(filePath);
                }
            }

            comm.Barrier();

            Logger.Info($"[Rank {comm.Rank}] StaticFiles: tasks = {myTaskFiles.Count}");

            comm.Barrier();

            Stopwatch sw = Stopwatch.StartNew();

            long localMatches = 0;

            StreamWriter? writer = null;

            if (saveToFile)
                writer = new StreamWriter($"results_rank_{comm.Rank}.txt", false, Encoding.UTF8);

            foreach (string filePath in myTaskFiles)
            {
                localMatches += ProcessWholeFileByLines(
                    filePath,
                    regex,
                    writer,
                    saveToFile,
                    comm.Rank);
            }

            writer?.Dispose();

            comm.Barrier();

            sw.Stop();

            long totalMatches = localMatches;

            if (comm.Rank == 0)
            {
                for (int rank = 1; rank < comm.Size; rank++)
                {
                    long part = comm.Receive<long>(rank, 330);
                    totalMatches += part;
                }

                Logger.Info("========================================");
                Logger.Info("StaticFiles finished");
                Logger.Info($"Local node matches: {totalMatches}");
                Logger.Info($"Elapsed: {sw.Elapsed.TotalSeconds:F3} sec");
                Logger.Info("========================================");

                return totalMatches;
            }
            else
            {
                comm.Send(localMatches, 0, 330);
                return 0;
            }
        }

        public static long RunDynamicOffsetMaster(
            Communicator comm,
            string filePath,
            long chunkSizeBytes)
        {
            Queue<long> taskQueue = CreateOffsetQueue(filePath, chunkSizeBytes);

            int totalTasks = taskQueue.Count;
            int completedTasks = 0;
            int activeWorkers = comm.Size - 1;

            long totalMatches = 0;

            Logger.Info($"[MASTER] DynamicFileOffsets: tasks = {totalTasks}");

            while (activeWorkers > 0)
            {
                Status status = comm.Probe(Communicator.anySource, Communicator.anyTag);

                if (status.Tag == (int)MsgTag.DynamicTaskRequest)
                {
                    int dummy;
                    comm.Receive(status.Source, (int)MsgTag.DynamicTaskRequest, out dummy);

                    if (taskQueue.Count > 0)
                    {
                        long nextOffset = taskQueue.Dequeue();
                        comm.Send(nextOffset, status.Source, (int)MsgTag.DynamicTaskResponse);
                    }
                    else
                    {
                        long stopSignal = -1;
                        comm.Send(stopSignal, status.Source, (int)MsgTag.DynamicTaskResponse);
                        activeWorkers--;
                    }
                }
                else if (status.Tag == (int)MsgTag.DynamicResultReport)
                {
                    long workerResult;
                    comm.Receive(status.Source, (int)MsgTag.DynamicResultReport, out workerResult);

                    totalMatches += workerResult;
                    completedTasks++;

                    Logger.Info(
                        $"[MASTER] DynamicFileOffsets: completed {completedTasks}/{totalTasks}, " +
                        $"totalMatches={totalMatches}");
                }
            }

            return totalMatches;
        }

        public static void RunDynamicOffsetWorker(
            Communicator comm,
            string filePath,
            Regex regex,
            long chunkSizeBytes)
        {
            int rank = comm.Rank;

            Logger.Info($"[Rank {rank}] DynamicFileOffsets worker started");

            using FileStream fs = new(
                filePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                bufferSize: 1024 * 1024,
                FileOptions.SequentialScan);

            byte[] buffer = new byte[4 * 1024 * 1024];
            Decoder decoder = Encoding.UTF8.GetDecoder();
            char[] charBuffer = new char[buffer.Length];

            while (true)
            {
                comm.Send(0, 0, (int)MsgTag.DynamicTaskRequest);

                long offset;
                comm.Receive(0, (int)MsgTag.DynamicTaskResponse, out offset);

                if (offset == -1)
                {
                    Logger.Info($"[Rank {rank}] DynamicFileOffsets stop signal");
                    break;
                }

                Stopwatch sw = Stopwatch.StartNew();

                long matches = ProcessFileChunkByLines(
                    fs,
                    offset,
                    chunkSizeBytes,
                    regex,
                    buffer,
                    decoder,
                    charBuffer);

                sw.Stop();

                Logger.Info(
                    $"[Rank {rank}] DynamicFileOffsets chunk offset={offset}, " +
                    $"matches={matches}, time={sw.ElapsedMilliseconds}ms");

                comm.Send(matches, 0, (int)MsgTag.DynamicResultReport);
            }
        }

        private static void SendString(
            Communicator comm,
            int targetRank,
            string value)
        {
            byte[] buffer = Encoding.UTF8.GetBytes(value);
            int length = buffer.Length;

            comm.Send(length, targetRank, (int)MsgTag.StaticFilePathLength);
            comm.Send(buffer, targetRank, (int)MsgTag.StaticFilePathData);
        }

        private static Queue<long> CreateOffsetQueue(string filePath, long chunkSizeBytes)
        {
            Queue<long> offsets = new();

            long fileSize = new FileInfo(filePath).Length;
            long currentOffset = 0;

            while (currentOffset < fileSize)
            {
                offsets.Enqueue(currentOffset);
                currentOffset += chunkSizeBytes;
            }

            return offsets;
        }

        private static long ProcessWholeFileByLines(
            string filePath,
            Regex regex,
            StreamWriter? writer,
            bool saveToFile,
            int rank)
        {
            long matches = 0;

            try
            {
                using StreamReader reader = new(
                    filePath,
                    Encoding.UTF8,
                    detectEncodingFromByteOrderMarks: true);

                string? line;
                int lineNumber = 0;

                while ((line = reader.ReadLine()) != null)
                {
                    lineNumber++;

                    if (regex.IsMatch(line))
                    {
                        matches++;

                        if (saveToFile && writer != null)
                        {
                            writer.WriteLine($"[{Path.GetFileName(filePath)}:{lineNumber}] {line}");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.Info($"[Rank {rank}] Skip file '{filePath}': {ex.Message}");
            }

            return matches;
        }

        private static long ProcessFileChunkByLines(
            FileStream fs,
            long offset,
            long chunkSizeBytes,
            Regex regex,
            byte[] buffer,
            Decoder decoder,
            char[] charBuffer)
        {
            long matches = 0;

            long fileLength = fs.Length;
            long end = Math.Min(offset + chunkSizeBytes, fileLength);

            fs.Seek(offset, SeekOrigin.Begin);

            
            if (offset != 0)
            {
                int b;

                while ((b = fs.ReadByte()) != -1)
                {
                    if (b == '\n')
                        break;
                }
            }

            decoder.Reset();

            StringBuilder lineBuilder = new(1024);

            long currentPos = fs.Position;

            while (currentPos < end)
            {
                int bytesToRead = (int)Math.Min(buffer.Length, end - currentPos);
                int bytesRead = fs.Read(buffer, 0, bytesToRead);

                if (bytesRead == 0)
                    break;

                int charsDecoded = decoder.GetChars(
                    buffer,
                    0,
                    bytesRead,
                    charBuffer,
                    0,
                    flush: false);

                for (int i = 0; i < charsDecoded; i++)
                {
                    char c = charBuffer[i];

                    if (c == '\n')
                    {
                        string line = lineBuilder.ToString();
                        lineBuilder.Clear();

                        if (regex.IsMatch(line))
                            matches++;
                    }
                    else if (c != '\r')
                    {
                        lineBuilder.Append(c);
                    }
                }

                currentPos = fs.Position;
            }

            
            int nextByte;

            while ((nextByte = fs.ReadByte()) != -1)
            {
                char c = (char)nextByte;

                if (c == '\n')
                {
                    string line = lineBuilder.ToString();
                    lineBuilder.Clear();

                    if (regex.IsMatch(line))
                        matches++;

                    break;
                }
                else if (c != '\r')
                {
                    lineBuilder.Append(c);
                }
            }

            return matches;
        }

        private static List<string> GetFilesSafe(string rootPath)
        {
            List<string> files = new();

            try
            {
                if (File.Exists(rootPath))
                {
                    files.Add(rootPath);
                    return files;
                }

                files.AddRange(Directory.GetFiles(rootPath));

                foreach (string directory in Directory.GetDirectories(rootPath))
                    files.AddRange(GetFilesSafe(directory));
            }
            catch (UnauthorizedAccessException)
            {
                
            }
            catch (Exception ex)
            {
                Logger.Info($"GetFilesSafe error: {ex.Message}");
            }

            return files;
        }
    }
}
