using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Text.Json;

namespace VKR_MPI_V1
{
    public class AppConfig
    {
        public string FilePath { get; set; }
        public string Pattern { get; set; }

        public int ChunkSizeMB { get; set; } = 128;
        public int OverlapKB { get; set; } = 32;

        public int WorkerThreads { get; set; } = Environment.ProcessorCount - 1;
    }
    

}
