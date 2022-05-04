using System.Text.Json;
namespace teichert.raft;

public class RaftMessage
{
    public MessageType MessageType { get; set; }
    public AppendEntriesRPCInfo? AppendEntriesRPCInfo { get; set; }
    public RequestVoteRPCInfo? RequestVoteRPCInfo { get; set; }
    public RPCResponseInfo? RPCResponseInfo { get; set; }
    public RaftCommand? RaftCommand { get; set; }

    public static RaftMessage FromJson(string json)
    {
        return JsonSerializer.Deserialize<RaftMessage>(json)!;
    }
    public string Json()
    {
        return JsonSerializer.Serialize(this);
    }
}

