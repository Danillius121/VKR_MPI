using MPI;
using System.Diagnostics;
using System.Text;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using VKR_MPI_V1;

namespace VKR_MPI_V1
{
    internal static class Program
    {
        private static int Main(string[] args)
        {
            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);

            using var mpiEnv = new MPI.Environment(ref args);
            var world = Communicator.world;

            try
            {
                var config = ConfigLoader.Load("appconfig.json");
                NodeContext topo = NodeTopology.Initialize(world);

                if (topo.IsNodeLeader)
                {
                    string? inputPath = System.Environment.GetEnvironmentVariable("NODE_INPUT_PATH");

                    if (!string.IsNullOrWhiteSpace(inputPath))
                        config.FilePath = inputPath;
                    else if (string.IsNullOrWhiteSpace(config.FilePath))
                        throw new InvalidOperationException("Config.FilePath is empty and NODE_INPUT_PATH is not set.");
                }

                DistributionStrategy strategy =
                    Enum.TryParse<DistributionStrategy>(
                        config.DistributionStrategy,
                        ignoreCase: true,
                        out var parsedStrategy)
                            ? parsedStrategy
                            : DistributionStrategy.DynamicSingleReader;

                switch (strategy)
                {
                    case DistributionStrategy.DynamicSingleReader:
                        {
                            if (topo.IsNodeLeader)
                            {
                                long localMatches = MasterNode.Run(topo.NodeComm, config);

                                if (world.Rank != 0)
                                {
                                    world.Send(localMatches, 0, 9100);
                                }
                                else
                                {
                                    long total = localMatches;

                                    for (int i = 1; i < topo.NodeCount; i++)
                                    {
                                        total += world.Receive<long>(Communicator.anySource, 9100);
                                    }

                                    Logger.Info($"Cluster total matches: {total}");

                                    File.WriteAllText(
                                        config.OutputPath,
                                        $"Strategy: {strategy}{System.Environment.NewLine}" +
                                        $"Pattern: {config.Pattern}{System.Environment.NewLine}" +
                                        $"Input: {config.FilePath}{System.Environment.NewLine}" +
                                        $"Total matches: {total}{System.Environment.NewLine}" +
                                        $"Nodes: {topo.NodeCount}{System.Environment.NewLine}");
                                }
                            }
                            else
                            {
                                WorkerNode.Run(topo.NodeComm, config);
                            }

                            break;
                        }

                    case DistributionStrategy.StaticFiles:
                        {
                            Regex regex = RegexFactory.Create(config);

                            long localMatches = AdditionalDistributionStrategies.RunStaticFileDistribution(
                                topo.NodeComm,
                                config.FilePath,
                                regex,
                                saveToFile: false);

                            if (topo.IsNodeLeader)
                            {
                                if (world.Rank != 0)
                                {
                                    world.Send(localMatches, 0, 9100);
                                }
                                else
                                {
                                    long total = localMatches;

                                    for (int i = 1; i < topo.NodeCount; i++)
                                    {
                                        total += world.Receive<long>(Communicator.anySource, 9100);
                                    }

                                    Logger.Info($"Cluster total matches: {total}");

                                    File.WriteAllText(
                                        config.OutputPath,
                                        $"Strategy: {strategy}{System.Environment.NewLine}" +
                                        $"Pattern: {config.Pattern}{System.Environment.NewLine}" +
                                        $"Input: {config.FilePath}{System.Environment.NewLine}" +
                                        $"Total matches: {total}{System.Environment.NewLine}" +
                                        $"Nodes: {topo.NodeCount}{System.Environment.NewLine}");
                                }
                            }

                            break;
                        }

                    case DistributionStrategy.DynamicFileOffsets:
                        {
                            Regex regex = RegexFactory.Create(config);

                            if (topo.IsNodeLeader)
                            {
                                Stopwatch sw = Stopwatch.StartNew();

                                long localMatches = AdditionalDistributionStrategies.RunDynamicOffsetMaster(
                                    topo.NodeComm,
                                    config.FilePath,
                                    config.DynamicOffsetChunkSizeBytes);

                                sw.Stop();

                                if (world.Rank != 0)
                                {
                                    world.Send(localMatches, 0, 9100);
                                }
                                else
                                {
                                    long total = localMatches;

                                    for (int i = 1; i < topo.NodeCount; i++)
                                    {
                                        total += world.Receive<long>(Communicator.anySource, 9100);
                                    }

                                    Logger.Info($"Cluster total matches: {total}");
                                    Logger.Info($"Elapsed: {sw.Elapsed.TotalSeconds:F3} sec");

                                    File.WriteAllText(
                                        config.OutputPath,
                                        $"Strategy: {strategy}{System.Environment.NewLine}" +
                                        $"Pattern: {config.Pattern}{System.Environment.NewLine}" +
                                        $"Input: {config.FilePath}{System.Environment.NewLine}" +
                                        $"Total matches: {total}{System.Environment.NewLine}" +
                                        $"Nodes: {topo.NodeCount}{System.Environment.NewLine}");
                                }
                            }
                            else
                            {
                                AdditionalDistributionStrategies.RunDynamicOffsetWorker(
                                    topo.NodeComm,
                                    config.FilePath,
                                    regex,
                                    config.DynamicOffsetChunkSizeBytes);
                            }

                            break;
                        }

                    default:
                        throw new InvalidOperationException($"Unknown distribution strategy: {strategy}");
                }
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