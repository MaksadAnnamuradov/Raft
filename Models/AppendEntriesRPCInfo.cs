namespace teichert.raft;

public class AppendEntriesRPCInfo
{
    public int LeaderTerm { get; set; } // c.f. term
    public int LeaderIndex { get; set; } // c.f. leaderId
    public int EntriesAlreadySent { get; set; } // c.f. prevLogIndex
    public int LastTermSent { get; set; }  // c.f. prevLogTerm
    public LogEntry[] EntriesToAppend { get; set; } = new LogEntry[] { }; // c.f. entries[]
    public int SenderSafeEntries { get; set; } // c.f. leaderCommit
}

