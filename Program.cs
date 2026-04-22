using MPI;
using System.Diagnostics;
using System.Text;
using VKR_MPI_V1;
using System.Text;
using System.Text.Json;

namespace VKR_MPI_V1
{
    internal static class Program
    {
        private static int Main(string[] args)
        {
            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);

            using var env = new MPI.Environment(ref args);
            var world = Communicator.world;

            try
            {
                var config = ConfigLoader.Load("appconfig.json");

                if (world.Rank == 0)
                    MasterNode.Run(world, config);
                else
                    WorkerNode.Run(world, config);

                return 0;
            }
            catch (Exception ex)
            {
                Logger.Error($"Rank {world.Rank}: fatal error: {ex}");
                return 1;
            }
        }
    }

    internal static class ConfigLoader
    {
        public static AppConfig Load(string fileName)
        {
            string path = Path.IsPathRooted(fileName)
                ? fileName
                : Path.Combine(AppContext.BaseDirectory, fileName);

            if (!File.Exists(path))
                throw new FileNotFoundException($"Config file not found: {path}");

            var options = new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true,
                ReadCommentHandling = JsonCommentHandling.Skip,
                AllowTrailingCommas = true
            };

            string json = File.ReadAllText(path, Encoding.UTF8);
            var config = JsonSerializer.Deserialize<AppConfig>(json, options)
                         ?? throw new InvalidOperationException("Failed to deserialize appconfig.json.");

            config.Validate();
            return config;
        }
    }
}