using MPI; // Не забудь добавить ссылку на библиотеку через NuGet
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Text.Json;

namespace VKR_MPI_V1
{
    class Program
    {
        static void Main(string[] args)
        {
            using (new MPI.Environment(ref args))
            {

                var comm = Communicator.world;
                var config = LoadConfig();

                List<string> files = new List<string>();

                // 1. Только Master (Rank 0) сканирует директорию
                if (comm.Rank == 0)
                {
                    files = GetFilesSafe(config.FilePath);
                    Console.WriteLine($"[MASTER]: Найдено файлов: {files.Count}");
                }

                // 2. Рассылаем список файлов всем процессам
                comm.Broadcast(ref files, 0);

                long localMatches = 0;

                // 3. Каждый процесс обрабатывает только "свои" файлы
                for (int i = 0; i < files.Count; i++)
                {
                    if (i % comm.Size == comm.Rank)
                    {
                        // Передаем конкретный файл в ProcessFile
                        localMatches += Pipeline.Consumer.ProcessFile(files[i], config);
                    }
                }

                // 4. Собираем результаты со всех узлов
                long globalTotal = comm.Reduce(localMatches, Operation<long>.Add, 0);

                if (comm.Rank == 0)
                {
                    Console.WriteLine($"Total matches: {globalTotal}");
                }

            }
        }

        static AppConfig LoadConfig(string path = "appconfig.json")
        {
            if (!File.Exists(path))
                return new AppConfig();

            string json = File.ReadAllText(path);
            return JsonSerializer.Deserialize<AppConfig>(json) ?? new AppConfig();
        }
        static List<string> GetFilesSafe(string path)
        {
            var result = new List<string>();
            try
            {
                // Проверяем, если указан конкретный файл, а не папка
                if (File.Exists(path))
                {
                    result.Add(path);
                    return result;
                }

                result.AddRange(Directory.GetFiles(path));
                foreach (var dir in Directory.GetDirectories(path))
                {
                    result.AddRange(GetFilesSafe(dir));
                }
            }
            catch (Exception) { /* игнорируем ошибки доступа */ }
            return result;
        }
    
    }
}