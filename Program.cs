using MPI; // Не забудь добавить ссылку на библиотеку через NuGet
using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Diagnostics;
using System.IO;
using System.Linq;

class Program
{
    static void Main(string[] args)
    {
        using (new MPI.Environment(ref args))
        {
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

                Console.WriteLine("\nВыберите способ вывода результатов:");
                Console.WriteLine("1. Вывод в консоль (с подсветкой)");
                Console.WriteLine("2. Сохранение в файлы (рекомендуется для больших объемов)");
                Console.Write("Ваш выбор: ");

                if (!int.TryParse(Console.ReadLine(), out outputMode)) outputMode = 1;
                modeBuffer[0] = outputMode;
            }

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
