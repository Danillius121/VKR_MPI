using MPI;
using System.Text;

namespace VKR_MPI_V1;

internal static class TopologyTags
{
    public const int HostNameLength = 9001;
    public const int HostNameData = 9002;
    public const int NodeColor = 9003;
    public const int NodeCount = 9004;
}

internal sealed class NodeContext
{
    public string HostName { get; }
    public int NodeColor { get; }
    public int NodeCount { get; }
    public Communicator NodeComm { get; }

    public NodeContext(string hostName, int nodeColor, int nodeCount, Communicator nodeComm)
    {
        HostName = hostName;
        NodeColor = nodeColor;
        NodeCount = nodeCount;
        NodeComm = nodeComm;
    }

    public bool IsNodeLeader => NodeComm.Rank == 0;
}

internal static class NodeTopology
{
    public static NodeContext Initialize(Communicator world)
    {
        string hostName = System.Environment.MachineName;
        int color;
        int nodeCount;

        if (world.Rank == 0)
        {
            var hostByRank = new string[world.Size];
            hostByRank[0] = hostName;

            for (int r = 1; r < world.Size; r++)
            {
                int len = world.Receive<int>(r, TopologyTags.HostNameLength);

                byte[] bytes = new byte[len];
                world.Receive(r, TopologyTags.HostNameData, ref bytes);

                if (bytes.Length != len)
                    throw new InvalidDataException(
                        $"Hostname length mismatch for rank {r}: expected {len}, got {bytes.Length}");

                hostByRank[r] = Encoding.UTF8.GetString(bytes);
            }

            var colorByHost = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            var colorByRank = new int[world.Size];
            int nextColor = 0;

            for (int r = 0; r < world.Size; r++)
            {
                string host = hostByRank[r];
                if (!colorByHost.TryGetValue(host, out int c))
                {
                    c = nextColor++;
                    colorByHost[host] = c;
                }

                colorByRank[r] = c;
            }

            nodeCount = nextColor;

            for (int r = 1; r < world.Size; r++)
                world.Send(colorByRank[r], r, TopologyTags.NodeColor);

            for (int r = 1; r < world.Size; r++)
                world.Send(nodeCount, r, TopologyTags.NodeCount);

            color = colorByRank[0];
        }
        else
        {
            byte[] bytes = Encoding.UTF8.GetBytes(hostName);

            world.Send(bytes.Length, 0, TopologyTags.HostNameLength);
            world.Send(bytes, 0, TopologyTags.HostNameData);

            color = world.Receive<int>(0, TopologyTags.NodeColor);
            nodeCount = world.Receive<int>(0, TopologyTags.NodeCount);
        }

        Communicator nodeComm = world.Split(color, world.Rank);
        return new NodeContext(hostName, color, nodeCount, nodeComm);
    }
}