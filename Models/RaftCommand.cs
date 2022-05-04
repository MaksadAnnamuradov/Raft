using System.Net;
namespace teichert.raft;

/// <summary>Represents a query/update to the statemachine by a particular client (who will eventually need the response)</summary>
public class RaftCommand
{
    public CommandType CommandType { set; get; }
    public string? Key { set; get; } = null;
    public int Value { set; get; }
    public IPEndPoint? Client { get; set; }
}

