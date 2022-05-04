namespace teichert.raft;

public class RequestVoteRPCInfo
{
    public int CandidateTerm { get; set; } // c.f. term
    public int CandidateIndex { get; set; } // c.f. candidateId
    public int CandidateLogLength { get; set; } // c.f. lastLogIndex
    public int CandidateLogTerm { get; set; } // c.f. lastLogterm
}

