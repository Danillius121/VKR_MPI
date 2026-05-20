using MPI; 
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

class Program
{
    // Теги для MPI сообщений
    enum MsgTag
    {
        TaskRequest = 100,  // Рабочий просит работу
        TaskResponse = 101, // Мастер дает (Offset)
        TaskFinish = 102,   // Мастер говорит "Работ больше нет"
        ResultReport = 103  // Воркер отправляет количество найденных строк
    }

    // Глобальные настройки
    static long minSizeInBytes = 256 * 1024 * 1024; // По умолчанию 256 МБ

    static readonly object ConsoleLock = new object();
    static StreamWriter? traceWriter;

    static void Trace(int rank, string message)
    {
        string line = $"[{DateTime.UtcNow:O}] [Rank {rank}] {message}";
        lock (ConsoleLock)
        {
            Console.WriteLine(line);
        }
        traceWriter?.WriteLine(line);
        traceWriter?.Flush();
    }

    static void Main(string[] args)
    {
        using (new MPI.Environment(ref args))
        {
            int processingMode = 1; // 1 - Много файлов, 2 - Один большой файл
            int outputMode = 1; // 1 - Консоль, 2 - Файл
            int[] modeBuffer = new int[1];

            Intracommunicator comm = Communicator.world;

            traceWriter = new StreamWriter($"trace_rank_{comm.Rank}.log", false)
            {
                AutoFlush = true
            };
            string targetPath = "";
            int pathLength = 0;
            byte[] pathData = null;
            if (comm.Rank == 0)
            {
                Console.Write("[MASTER] Введите путь: ");
                targetPath = Console.ReadLine() ?? "";
                pathData = System.Text.Encoding.UTF8.GetBytes(targetPath);
                pathLength = pathData.Length;

                Console.WriteLine("\nВЫБОР СТРАТЕГИИ РАСПРЕДЕЛЕНИЯ");
                Console.WriteLine("1. Раздача по целым файлам (File-per-Worker)");
                Console.WriteLine("2. Квантование одного файла (Dynamic Task Farm)");
                Console.Write("Выбор: ");
                if (!int.TryParse(Console.ReadLine(), out processingMode)) processingMode = 1;

                Console.WriteLine("\nВыберите способ вывода результатов:");
                Console.WriteLine("1. Вывод в консоль (с подсветкой)");
                Console.WriteLine("2. Сохранение в файлы (рекомендуется для больших объемов)");
                Console.Write("Ваш выбор: ");

                if (!int.TryParse(Console.ReadLine(), out outputMode)) outputMode = 1;
                modeBuffer[0] = outputMode;
            }

            string pattern = "";
            byte[] patternData = null;
            int patternLength = 0;

            if (comm.Rank == 0)
            {
                Console.Write("[MASTER] Введите регулярное выражение (RegEx): ");
                pattern = Console.ReadLine() ?? "";
                patternData = System.Text.Encoding.UTF8.GetBytes(pattern);
                patternLength = patternData.Length;
            }

            // Передаем длину паттерна всем узлам
            comm.Broadcast(ref patternLength, 0);

            // Рабочие готовят буфер
            if (comm.Rank != 0) patternData = new byte[patternLength];

            // Передаем сами байты паттерна
            comm.Broadcast(ref patternData, 0);

            // Восстанавливаем строку на рабочих процессах
            if (comm.Rank != 0) pattern = System.Text.Encoding.UTF8.GetString(patternData);
              
            // Инициализируем Regex
            Regex regex = new Regex(pattern, RegexOptions.Compiled | RegexOptions.IgnoreCase);


            // Рассылаем выбор всем
            int[] modeBuf = { processingMode };
            comm.Broadcast(ref modeBuf, 0);
            processingMode = modeBuf[0];

            // Передаем массив из одного целого числа
            comm.Broadcast(ref modeBuffer, 0);

            // Все рабочие забирают значение из буфера
            if (comm.Rank != 0) outputMode = modeBuffer[0];

            bool saveToFile = (outputMode == 2);

            // Барьер для фиксации настроек на всех узлах
            comm.Barrier();

            // 1. Сначала все синхронно обмениваются длиной строки
            comm.Broadcast(ref pathLength, 0);

            // 2. Теперь рабочие готовят буфер нужного размера
            if (comm.Rank != 0) pathData = new byte[pathLength];

            // 3. Все синхронно передают/принимают сами байты
            comm.Broadcast(ref pathData, 0);

            // 4. Рабочие восстанавливают строку из байт
            if (comm.Rank != 0) targetPath = System.Text.Encoding.UTF8.GetString(pathData);

            // ВАЖНО: Добавь Barrier, чтобы никто не начал сканировать диск раньше времени
            comm.Barrier();

            if (string.IsNullOrEmpty(targetPath)) return;
            //-----------------------------------------------------------------------------
            if (processingMode == 1)
            {
                
                RunMultiFileSearch(comm, targetPath, saveToFile, regex);
            }
            else
            {
                
                if (comm.Rank == 0)
                {
                    Stopwatch totalSw = Stopwatch.StartNew();
                    
                    long totalMatches = Master_ScheduleLargeFile(comm, targetPath, minSizeInBytes);
                    totalSw.Stop();
                    
                    PrintFinalResult(totalMatches, totalSw.Elapsed.TotalSeconds);
                }
                else
                {
                    // Рабочие уходят в цикл запроса задач
                    Worker_ProcessLargeFile(comm, targetPath, regex, minSizeInBytes);
                    traceWriter?.Dispose();
                }
            }




        }


    }

    static void RunMultiFileSearch(Intracommunicator comm, string targetPath, bool saveToFile, Regex regex)
    {

        
        List<string> myTaskFiles = new List<string>();

        if (comm.Rank == 0)
        {
            Console.WriteLine("[MASTER] Сканирую директорию...");
            List<string> allFiles = GetFilesSafe(targetPath);
            Console.WriteLine($"[MASTER] Найдено файлов: {allFiles.Count}");

            
            for (int i = 0; i < allFiles.Count; i++)
            {
                int targetRank = i % comm.Size;
                if (targetRank == 0)
                {
                    myTaskFiles.Add(allFiles[i]);
                }
                else
                {
                    // Ручная передача строки через байты
                    byte[] buffer = System.Text.Encoding.UTF8.GetBytes(allFiles[i]);
                    int length = buffer.Length;
                    comm.Send(length, targetRank, 10); // Тег 10 - длина
                    comm.Send(buffer, targetRank, 11); // Тег 11 - данные
                }
            }

            // Сигнал завершения (длина -1)
            for (int r = 1; r < comm.Size; r++)
            {
                int stopSignal = -1;
                comm.Send(stopSignal, r, 10);
            }
        }
        else
        {
            // Рабочие принимают байты и превращают их обратно в строки
            while (true)
            {
                int length = 0;
                comm.Receive(0, 10, out length); 
                if (length == -1) break; // Стоп-сигнал

                byte[] buffer = new byte[length];
                comm.Receive(0, 11, ref buffer); 
                string receivedPath = System.Text.Encoding.UTF8.GetString(buffer);
                myTaskFiles.Add(receivedPath);
            }
        }


        // Синхронизация перед началом поиска
        comm.Barrier();
        Console.WriteLine($"[Rank {comm.Rank}] Готов к поиску. Задач: {myTaskFiles.Count}");




        if (comm.Rank == 0) Console.WriteLine(">>> Все узлы синхронизированы. Начинаем распределение файлов...");

        
        // 1. СИНХРОНИЗАЦИЯ ПЕРЕД СТАРТОМ
        comm.Barrier();
        Stopwatch totalSw = Stopwatch.StartNew();

        long localMatches = 0;
        
        StreamWriter writer = null;
        if (saveToFile)
        {
            
            writer = new StreamWriter($"results_rank_{comm.Rank}.txt", false);
        }

        foreach (var filePath in myTaskFiles)
        {
            try
            {
                using (var reader = new StreamReader(filePath))
                {
                    string line;
                    int lineNumber = 0;
                    while ((line = reader.ReadLine()) != null)
                    {
                        lineNumber++;
                        if (regex.IsMatch(line))
                        {
                            localMatches++;

                            if (saveToFile)
                            {
                                writer.WriteLine($"[{Path.GetFileName(filePath)} : {lineNumber}] {line}");
                            }
                            else
                            {
                                
                                lock (Console.Out) 
                                {
                                    Console.ForegroundColor = ConsoleColor.Cyan;
                                    Console.Write($"[Rank {comm.Rank}] ");
                                    Console.ResetColor();
                                    Console.Write($"{Path.GetFileName(filePath)}:{lineNumber} > ");

                                    HighlightAndPrint(line, regex);
                                }
                            }
                        }
                    }
                }
            }
            catch { /* Пропуск ошибок доступа */ }
        }

        if (writer != null) writer.Close();

        // 2. СИНХРОНИЗАЦИЯ ПОСЛЕ ПОИСКА
        comm.Barrier();
        totalSw.Stop();

        // Сбор суммы (Reduce)
        long totalMatches = comm.Reduce(localMatches, Operation<long>.Add, 0);

        if (comm.Rank == 0)
        {
            Console.WriteLine("\n" + new string('=', 40));
            Console.WriteLine($" ПОЛНОЕ ВРЕМЯ РАБОТЫ: {totalSw.Elapsed.TotalSeconds:F3} сек.");
            Console.WriteLine($" Найдено всего: {totalMatches}");
            if (saveToFile) Console.WriteLine(" Результаты сохранены в раздельные файлы по рангам.");
            Console.WriteLine(new string('=', 40));
        }

        // Метод для подсветки
        static void HighlightAndPrint(string line, Regex regex)
        {
            int lastIndex = 0;
            foreach (Match m in regex.Matches(line))
            {
                Console.Write(line.Substring(lastIndex, m.Index - lastIndex));
                Console.BackgroundColor = ConsoleColor.DarkYellow;
                Console.ForegroundColor = ConsoleColor.Black;
                Console.Write(m.Value);
                Console.ResetColor();
                lastIndex = m.Index + m.Length;
            }
            Console.WriteLine(line.Substring(lastIndex));
        }
    }

    static Queue<long> CreateTaskQueue(string filePath, long minSize)
    {
        Queue<long> offsets = new Queue<long>();
        long fileSize = new FileInfo(filePath).Length;
        long currentOffset = 0;

        while (currentOffset < fileSize)
        {
            offsets.Enqueue(currentOffset);
            currentOffset += minSize;

            // Если остаток меньше минимального размера, 
            // не создаем новый маленький блок
            if (fileSize - currentOffset < minSize) break;
        }
        return offsets;
    }

    static long Master_ScheduleLargeFile(Intracommunicator comm, string filePath, long minSize)
    {
        // 1. Создаем очередь задач
        Queue<long> taskQueue = CreateTaskQueue(filePath, minSize);
        int totalTasks = taskQueue.Count;
        int completedTasks = 0;
        long totalMatches = 0;

        Console.WriteLine($"[MASTER] Создано задач: {totalTasks}. Раздаю воркерам...");

        // 2. Цикл обработки запросов
        // Продолжаем, пока не закроем все задачи и не получим отчеты от всех рабочих
        int activeWorkers = comm.Size - 1;

        while (activeWorkers > 0)
        {
            // Ждем сообщение от любого рабочего
            Status status = comm.Probe(Communicator.anySource, Communicator.anyTag);

            if (status.Tag == (int)MsgTag.TaskRequest)
            {
                // Рабочий просит задачу
                int dummy;
                comm.Receive(status.Source, (int)MsgTag.TaskRequest, out dummy);

                if (taskQueue.Count > 0)
                {
                    long nextOffset = taskQueue.Dequeue();
                    comm.Send(nextOffset, status.Source, (int)MsgTag.TaskResponse);
                }
                else
                {
                    // Задач нет - шлем сигнал финиша
                    long stopSignal = -1;
                    comm.Send(stopSignal, status.Source, (int)MsgTag.TaskResponse);
                    activeWorkers--; 
                }
            }
            else if (status.Tag == (int)MsgTag.ResultReport)
            {
                
                long workerResult;
                comm.Receive(status.Source, (int)MsgTag.ResultReport, out workerResult);
                totalMatches += workerResult;
                completedTasks++;
            }
        }

        return totalMatches;
    }

    static void Worker_ProcessLargeFile(
    Intracommunicator comm,
    string filePath,
    Regex regex,
    long chunkSize)
    {
        int rank = comm.Rank;
        Trace(rank, $"Worker started. file={filePath}, chunkSize={chunkSize}");

        // Один FileStream на весь worker
        using FileStream fs = new FileStream(
            filePath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            bufferSize: 1024 * 1024,
            FileOptions.SequentialScan);

        byte[] buffer = new byte[4 * 1024 * 1024]; // 4 MB буфер
        Decoder decoder = Encoding.UTF8.GetDecoder();
        char[] charBuffer = new char[buffer.Length];

        while (true)
        {
            
            comm.Send(0, 0, (int)MsgTag.TaskRequest);

            long offset;
            comm.Receive(0, (int)MsgTag.TaskResponse, out offset);

            if (offset == -1)
            {
                Trace(rank, "Stop signal received");
                break;
            }

            Stopwatch sw = Stopwatch.StartNew();

            long matches = 0;
            long end = Math.Min(offset + chunkSize, fs.Length);

            
            fs.Seek(offset, SeekOrigin.Begin);

            
            if (offset != 0)
            {
                int b;
                while ((b = fs.ReadByte()) != -1)
                {
                    if (b == '\n') break;
                }
            }

            StringBuilder lineBuilder = new StringBuilder(1024);

            long currentPos = fs.Position;

            while (currentPos < end)
            {
                int bytesToRead = (int)Math.Min(buffer.Length, end - currentPos);
                int bytesRead = fs.Read(buffer, 0, bytesToRead);

                if (bytesRead == 0)
                    break;

                int charsDecoded = decoder.GetChars(buffer, 0, bytesRead, charBuffer, 0, false);

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

            sw.Stop();

            Trace(rank, $"Chunk done offset={offset}, matches={matches}, time={sw.ElapsedMilliseconds}ms");

            comm.Send(matches, 0, (int)MsgTag.ResultReport);
        }
    }

    static void PrintFinalResult(long totalMatches, double elapsed)
    {
        Console.WriteLine("\n" + new string('=', 40));
        Console.ForegroundColor = ConsoleColor.Green;
        Console.WriteLine($" ПОИСК ЗАВЕРШЕН");
        Console.ResetColor();
        Console.WriteLine($" Найдено совпадений: {totalMatches}");
        Console.WriteLine($" Полное время работы: {elapsed:F3} сек.");
        Console.WriteLine(new string('=', 40));
        Console.WriteLine("Нажмите Enter для выхода...");
        Console.ReadLine();
    }


    static List<string> GetFilesSafe(string rootPath)
    {
        List<string> files = new List<string>();
        try
        {
            // Получаем файлы в текущей папке
            files.AddRange(Directory.GetFiles(rootPath));

            // Рекурсивно заходим в подпапки
            foreach (var directory in Directory.GetDirectories(rootPath))
            {
                files.AddRange(GetFilesSafe(directory));
            }
        }
        catch (UnauthorizedAccessException)
        {
            // Просто игнорируем папки, куда нас не пускает ОС
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Ошибка доступа: {ex.Message}");
        }
        return files;
    }
    
}
