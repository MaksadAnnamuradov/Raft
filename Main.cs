using System.Net;
using teichert.raft;

class RaftRunner
{
    static void Main(string[] endPointsAndIndex)
    {
        if (endPointsAndIndex.Length < 2)
        {
            Console.WriteLine("Please enter the IPEndPoints and your node index.");
            Console.WriteLine("For example: 127.0.0.1:54321 127.0.0.1:54322 127.0.0.1:54323 0");
            Console.Write("Enter here: ");
            endPointsAndIndex = Console.ReadLine()!.Split();
        }
        int nodeIndex = int.Parse(endPointsAndIndex[^1]);
        var allNodes = from endPointString in endPointsAndIndex.SkipLast(1)
                       select IPEndPoint.Parse(endPointString);
        RaftNode node = new RaftNode(allNodes.ToList(), nodeIndex);
        node.Start().Wait();
    }
}