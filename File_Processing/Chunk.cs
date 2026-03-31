using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace VKR_MPI_V1.File_Processing
{
    public class Chunk
    {
        public byte[] Buffer;
        public int Length;

        public long GlobalStart;   // начало чанка в файле
        public long ValidEnd;      // граница без overlap
    }
}
