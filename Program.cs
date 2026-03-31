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

                if (comm.Rank == 0)
                    MPI_Layer.Master.MasterMPI(comm, config);
                else
                    MPI_Layer.Worker.WorkerMPI(comm, config);
            }
        }

        static AppConfig LoadConfig(string path = "appconfig.json")
        {
            if (!File.Exists(path))
                return new AppConfig();

            string json = File.ReadAllText(path);
            return JsonSerializer.Deserialize<AppConfig>(json) ?? new AppConfig();
        }
    }
}