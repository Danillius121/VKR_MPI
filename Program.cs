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
            Intracommunicator comm = Communicator.world;

            string targetPath = "123";
            int pathLength = 0;
            byte[] pathData = null;

            if (comm.Rank == 0)
            {
                Console.Write("[MASTER] Введите путь: ");
                targetPath = Console.ReadLine() ?? "";
                pathData = System.Text.Encoding.UTF8.GetBytes(targetPath);
                pathLength = pathData.Length;
            }

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

            comm.Barrier();
            if (comm.Rank == 0) Console.WriteLine($">>> Начинаю поиск паттерна: '{pattern}'...");


            long localMatches = 0;
            Stopwatch sw = Stopwatch.StartNew();

            // Выполняем поиск
            foreach (var filePath in myTaskFiles)
            {
                try
                {
                    // Используем FileStream для более стабильного чтения в сетевых окружениях
                    using (FileStream fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                    using (StreamReader reader = new StreamReader(fs))
                    {
                        string line;
                        while ((line = reader.ReadLine()) != null)
                        {
                            if (regex.IsMatch(line))
                            {
                                localMatches++;
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    // Логируем ошибку, но НЕ выходим из программы
                    Console.WriteLine($"[Rank {comm.Rank}] Ошибка доступа: {Path.GetFileName(filePath)}");
                }
            }
            sw.Stop();

            // ТОЧКА СИНХРОНИЗАЦИИ: Ждем, пока ВСЕ закончат чтение своих файлов
            comm.Barrier();

            // ВАЖНО: Эту строку должны вызвать ВСЕ одновременно
            long totalMatches = 0;
            totalMatches = comm.Reduce(localMatches, Operation<long>.Add, 0);

            // Вывод итогов только на мастере
            if (comm.Rank == 0)
            {
                Console.WriteLine("\n" + new string('=', 40));
                Console.WriteLine($"✅ ПОИСК ЗАВЕРШЕН");
                Console.WriteLine($"📊 Найдено совпадений: {totalMatches}");
                Console.WriteLine($"⏱ Время (Rank 0): {sw.Elapsed.TotalSeconds:F3} сек.");
                Console.WriteLine(new string('=', 40));

                // На Rank 0 ReadLine допустим для удержания окна
                Console.WriteLine("Нажмите Enter для выхода...");
                Console.ReadLine();
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
