using MPI; // Не забудь добавить ссылку на библиотеку через NuGet
using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Diagnostics;
using System.IO;
using System.Linq;

class Program
{
    // Теги для MPI сообщений
    enum MsgTag
    {
        TaskRequest = 100,  // Воркер просит работу
        TaskResponse = 101, // Мастер дает (Offset)
        TaskFinish = 102,   // Мастер говорит "Работ больше нет"
        ResultReport = 103  // Воркер шлет количество найденных строк
    }

    // Глобальные настройки
    static long minSizeInBytes = 64 * 1024 * 1024; // По умолчанию 64 МБ


    static void Main(string[] args)
    {
        using (new MPI.Environment(ref args))
        {
            int processingMode = 1; // 1 - Много файлов, 2 - Один большой файл
            int outputMode = 1; // 1 - Консоль, 2 - Файл
            int[] modeBuffer = new int[1];

            Intracommunicator comm = Communicator.world;

            string targetPath = "";
            int pathLength = 0;
            byte[] pathData = null;
            if (comm.Rank == 0)
            {
                Console.Write("[MASTER] Введите путь: ");
                targetPath = Console.ReadLine() ?? "";
                pathData = System.Text.Encoding.UTF8.GetBytes(targetPath);
                pathLength = pathData.Length;

                Console.WriteLine("\n--- ВЫБОР СТРАТЕГИИ РАСПРЕДЕЛЕНИЯ ---");
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

            // Рассылаем выбор всем (через наш стабильный метод с массивом)
            int[] modeBuf = { processingMode };
            comm.Broadcast(ref modeBuf, 0);
            processingMode = modeBuf[0];

            // Передаем массив из одного целого числа (это самая стабильная операция в MPI.NET)
            comm.Broadcast(ref modeBuffer, 0);

            // Все воркеры забирают значение из буфера
            if (comm.Rank != 0) outputMode = modeBuffer[0];

            bool saveToFile = (outputMode == 2);

            // Барьер для фиксации настроек на всех узлах
            comm.Barrier();

            // 1. Сначала ВСЕ (и Мастер, и Воркеры) синхронно обмениваются длиной строки
            comm.Broadcast(ref pathLength, 0);

            // 2. Теперь Воркеры готовят буфер нужного размера
            if (comm.Rank != 0) pathData = new byte[pathLength];

            // 3. ВСЕ (и Мастер, и Воркеры) синхронно передают/принимают сами байты
            comm.Broadcast(ref pathData, 0);

            // 4. Воркеры восстанавливают строку из байт
            if (comm.Rank != 0) targetPath = System.Text.Encoding.UTF8.GetString(pathData);

            // ВАЖНО: Добавь Barrier, чтобы никто не начал сканировать диск раньше времени
            comm.Barrier();

            if (string.IsNullOrEmpty(targetPath)) return;
            //-----------------------------------------------------------------------------
            if (processingMode == 1)
            {
                // Тот код, который мы отладили (раздача путей через Send/Receive)
                RunMultiFileSearch(comm, targetPath, saveToFile);
            }
            else
            {
                // РЕЖИМ 2: Один большой файл (Новая логика по твоему плану)
                if (comm.Rank == 0)
                {
                    // Мастер становится Диспетчером
                    long totalMatches = Master_ScheduleLargeFile(comm, targetPath, minSizeInBytes);

                    // Вывод итогов только здесь
                    PrintFinalResult(totalMatches, sw.Elapsed);
                }
                else
                {
                    // Воркеры уходят в цикл запроса задач
                    Worker_ProcessLargeFile(comm, targetPath, regex, minSizeInBytes);
                }
            }




        }


    }

    static void RunMultiFileSearch(Intracommunicator comm, string targetPath, bool saveToFile)
    {

        // Теперь каждый процесс безопасно получает список доступных файлов
        List<string> myTaskFiles = new List<string>();

        if (comm.Rank == 0)
        {
            Console.WriteLine("[MASTER] Сканирую директорию...");
            List<string> allFiles = GetFilesSafe(targetPath);
            Console.WriteLine($"[MASTER] Найдено файлов: {allFiles.Count}");

            // Мастер раздает файлы воркерам
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
            // ВОРКЕРЫ: Принимают байты и превращают их обратно в строки
            while (true)
            {
                int length = 0;
                comm.Receive(0, 10, out length); // Ждем длину
                if (length == -1) break; // Стоп-сигнал

                byte[] buffer = new byte[length];
                comm.Receive(0, 11, ref buffer); // Ждем данные
                string receivedPath = System.Text.Encoding.UTF8.GetString(buffer);
                myTaskFiles.Add(receivedPath);
            }
        }


        // Синхронизация перед началом поиска
        comm.Barrier();
        Console.WriteLine($"[Rank {comm.Rank}] Готов к поиску. Задач: {myTaskFiles.Count}");




        if (comm.Rank == 0) Console.WriteLine(">>> Все узлы синхронизированы. Начинаем распределение файлов...");

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

        // Воркеры готовят буфер
        if (comm.Rank != 0) patternData = new byte[patternLength];

        // Передаем сами байты паттерна
        comm.Broadcast(ref patternData, 0);

        // Восстанавливаем строку на воркерах
        if (comm.Rank != 0) pattern = System.Text.Encoding.UTF8.GetString(patternData);

        // Инициализируем Regex (скомпилированный вариант для скорости)
        Regex regex = new Regex(pattern, RegexOptions.Compiled | RegexOptions.IgnoreCase);

        // 1. СИНХРОНИЗАЦИЯ ПЕРЕД СТАРТОМ
        comm.Barrier();
        Stopwatch totalSw = Stopwatch.StartNew();

        long localMatches = 0;
        // Инициализируем StreamWriter, если выбрано сохранение в файл
        StreamWriter writer = null;
        if (saveToFile)
        {
            // Каждый процесс пишет в свой файл: это "Best Practice" для MPI
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
                                // ВЫВОД В КОНСОЛЬ С ПОДСВЕТКОЙ
                                lock (Console.Out) // Минимальная защита от перемешивания строк
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
            Console.WriteLine($"✅ ПОЛНОЕ ВРЕМЯ РАБОТЫ: {totalSw.Elapsed.TotalSeconds:F3} сек.");
            Console.WriteLine($"📊 Найдено всего: {totalMatches}");
            if (saveToFile) Console.WriteLine("📂 Результаты сохранены в раздельные файлы по рангам.");
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

    static void RunSingleLargeFileSearch()
    {

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
            // мы не создаем новый маленький блок (Пункт 2)
            if (fileSize - currentOffset < minSize) break;
        }
        return offsets;
    }

    static long FindNextLineStart(FileStream fs, long startOffset)
    {
        if (startOffset == 0) return 0; // Первый блок всегда с начала

        fs.Seek(startOffset, SeekOrigin.Begin);
        // Читаем побайтово до конца строки
        int b;
        while ((b = fs.ReadByte()) != -1)
        {
            if (b == '\n') return fs.Position; // Возвращаем позицию СЛЕДУЮЩЕГО байта
        }
        return fs.Length; // Если \n не нашли до конца файла
    }
    /*
    static long Worker_ProcessLargeFile(Intracommunicator comm, string filePath, Regex regex, long minSize)
    {
        long localCount = 0;
        while (true)
        {
            // 1. Запрашиваем задачу у Мастера (Тег TaskRequest)
            int requestSignal = 1;
            comm.Send(requestSignal, 0, (int)MsgTag.TaskRequest);

            // 2. Ждем ответ: смещение (Offset) или сигнал финиша
            long startOffset;
            comm.Receive(0, (int)MsgTag.TaskResponse, out startOffset);

            // Если пришел -1, значит задач в очереди Мастера больше нет
            if (startOffset == -1) break;

            // 3. Работаем с блоком
            localCount += ProcessFileChunk(filePath, startOffset, minSize, regex);
        }
        return localCount;
    }
    */
    static long ProcessFileChunk(string path, long startOffset, long minSize, Regex regex)
    {
        long matches = 0;
        using (FileStream fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read))
        {
            // Находим реальный старт строки (Пункт 3 твоего плана)
            long actualStart = (startOffset == 0) ? 0 : FindNextLineStart(fs, startOffset);
            fs.Seek(actualStart, SeekOrigin.Begin);

            using (StreamReader sr = new StreamReader(fs))
            {
                string line;
                long endBoundary = startOffset + minSize;

                // Читаем, пока не пересечем границу MinimalSize 
                // и обязательно дочитываем последнюю строку до конца
                while (fs.Position < endBoundary && (line = sr.ReadLine()) != null)
                {
                    if (regex.IsMatch(line)) matches++;
                }

                // Дочитываем "хвост" строки за границей чанка (Пункт 2 плана)
                if (fs.Position >= endBoundary && (line = sr.ReadLine()) != null)
                {
                    if (regex.IsMatch(line)) matches++;
                }
            }
        }
        return matches;
    }

    static long Master_ScheduleLargeFile(Intracommunicator comm, string filePath, long minSize)
    {
        // 1. Создаем очередь задач (Квантование файла)
        Queue<long> taskQueue = CreateTaskQueue(filePath, minSize);
        int totalTasks = taskQueue.Count;
        int completedTasks = 0;
        long totalMatches = 0;

        Console.WriteLine($"[MASTER] Создано задач: {totalTasks}. Раздаю воркерам...");

        // 2. Цикл обработки запросов
        // Мы продолжаем, пока не закроем все задачи и не получим отчеты от всех воркеров
        int activeWorkers = comm.Size - 1;

        while (activeWorkers > 0)
        {
            // Ждем сообщение от любого воркера (тег TaskRequest или ResultReport)
            Status status = comm.Probe(Communicator.anySource, Communicator.anyTag);

            if (status.Tag == (int)MsgTag.TaskRequest)
            {
                // Воркер просит задачу
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
                    activeWorkers--; // Этот воркер больше не вернется
                }
            }
            else if (status.Tag == (int)MsgTag.ResultReport)
            {
                // Воркер прислал количество найденных строк в своем блоке
                long workerResult;
                comm.Receive(status.Source, (int)MsgTag.ResultReport, out workerResult);
                totalMatches += workerResult;
                completedTasks++;

                // Выводим прогресс на мастере (Пункт 5 твоего плана)
                if (totalTasks > 0)
                    DrawProgressBar(completedTasks, totalTasks);
            }
        }

        return totalMatches;
    }

    static void Worker_ProcessLargeFile(Intracommunicator comm, string filePath, Regex regex, long minSize)
    {
        while (true)
        {
            // Просим задачу
            comm.Send(0, 0, (int)MsgTag.TaskRequest);

            long startOffset;
            comm.Receive(0, (int)MsgTag.TaskResponse, out startOffset);

            if (startOffset == -1) break; // Финиш

            // Выполняем поиск в чанке
            long foundInChunk = ProcessFileChunk(filePath, startOffset, minSize, regex);

            // ОТПРАВЛЯЕМ РЕЗУЛЬТАТ МАСТЕРУ (Пункт 4 твоего плана)
            comm.Send(foundInChunk, 0, (int)MsgTag.ResultReport);
        }
    }

    static void PrintFinalResult(long totalMatches, TimeSpan elapsed)
    {
        Console.WriteLine("\n" + new string('=', 40));
        Console.ForegroundColor = ConsoleColor.Green;
        Console.WriteLine($"✅ ПОИСК ЗАВЕРШЕН");
        Console.ResetColor();
        Console.WriteLine($"📊 Найдено совпадений: {totalMatches}");
        Console.WriteLine($"⏱ Полное время работы: {elapsed.TotalSeconds:F3} сек.");

        // Для диплома: расчет теоретической пропускной способности
        // (если добавишь размер файла в аргументы)
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
