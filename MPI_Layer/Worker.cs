using MPI;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using VKR_MPI_V1.Pipeline;

namespace VKR_MPI_V1.MPI_Layer
{
    public class Worker
    {
        public static void WorkerMPI(Intracommunicator comm, AppConfig config)
        {
            //long localResult = Consumer.ProcessFile(config);

            //long global = comm.Reduce(localResult, Operation<long>.Add, 0);
        }
    }
}
