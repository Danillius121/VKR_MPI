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
            var files = Directory.GetFiles(config.FilePath, "*", SearchOption.AllDirectories);

            int size = comm.Size;

            for (int i = 0; i < files.Length; i++)
            {
                int target = i % size;
                comm.Send(files[i], target, 0);
            }

            long total = 0;
            long global = comm.Reduce(total, Operation<long>.Add, 0);

            Console.WriteLine($"Total matches: {global}");
        }
    }
}
