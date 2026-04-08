using MPI;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace VKR_MPI_V1.MPI_Layer
{
    public class Master
    {
        public static void MasterMPI(Intracommunicator comm, AppConfig config)
        {
            var files = GetFilesSafe(config.FilePath);
            Console.WriteLine("[MASTER]:");
            int size = comm.Size;

            for (int i = 0; i < files.Count; i++)
            {
                int target = i % size;
                comm.Send(files[i], target, 0);
            }

            long total = 0;
            long global = comm.Reduce(total, Operation<long>.Add, 0);

            Console.WriteLine($"Total matches: {global}");
        }

        static List<string> GetFilesSafe(string path)
        {
            var result = new List<string>();

            try
            {
                result.AddRange(Directory.GetFiles(path));
            }
            catch (Exception)
            {
                // игнорируем
            }

            try
            {
                foreach (var dir in Directory.GetDirectories(path))
                {
                    result.AddRange(GetFilesSafe(dir));
                }
            }
            catch (Exception)
            {
                // игнорируем
            }

            return result;
        }
    }
}
