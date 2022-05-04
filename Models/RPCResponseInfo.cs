namespace teichert.raft;

public class RPCResponseInfo
{
    public int CurrentTerm { get; set; } // c.f. term
    public bool Response { get; set; } // c.f. success / voteGranted
}

