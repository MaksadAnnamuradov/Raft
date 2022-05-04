using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
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

public class RPCResponseInfo
{
    public int CurrentTerm { get; set; } // c.f. term
    public bool Response { get; set; } // c.f. success / voteGranted
}

public class RequestVoteRPCInfo
{
    public int CandidateTerm { get; set; } // c.f. term
    public int CandidateIndex { get; set; } // c.f. candidateId
    public int CandidateLogLength { get; set; } // c.f. lastLogIndex
    public int CandidateLogTerm { get; set; } // c.f. lastLogterm
}

public class RaftNode
{
    public RaftState RaftState = RaftState.Follower;
    private List<IPEndPoint> allNodes;
    private int nodeIndex; // index of this node in the list of allNodes
    private int leaderIndex = 0; // index of leader node so that this node can forward requests to leader

    // PERSISTENT STATE
    private StateMachine stateMachine = new StateMachine();
    private int currentTerm = 0; // c.f. currentTerm
    private string currentTermPath => $"termPath.{nodeIndex}.txt";
    private int? votedFor = null; // c.f. votedFor
    private string votedForPath => $"votedFor.{nodeIndex}.txt";
    private object voteLock = new object(); // held when accessing votedFor
    private ConcurrentDictionary<int, bool> votesForMe = new();
    private List<LogEntry> logEntries = new(); // c.f. log[]
    private string logEntriesPath => $"raftLog.{nodeIndex}.txt";

    // VOLATILE STATE
    /// <summary>The number of log entries that have been replicated and may be applied to the state machine</summary>
    public int SafeEntries { get; set; } // c.f. commitIndex
    /// <summary>The number of log entries that have been applied to the state machine</summary>
    public int AppliedEntries { get; set; } = 0; // c.f. lastApplied

    // (VOLATILE) LEADER STATE
    private ConcurrentDictionary<int, int> possiblyReplicatedEntries = new(); // c.f. nextIndex[] (hoping that they got all of our)
    private ConcurrentDictionary<int, int> confirmedReplicatedEntries = new(); // c.f. matchIndex[] (waiting until we know they got all of ours)

    // FIXED / UTIL
    private const int TimeOutMultiplier = 100;
    private TimeSpanRange ElectionTimeOutRange = new TimeSpanRange(
        TimeSpan.FromMilliseconds(150 * TimeOutMultiplier),
        TimeSpan.FromMilliseconds(300 * TimeOutMultiplier));
    private TimeSpan HeartBeatTimeOut = TimeSpan.FromMilliseconds(50 * TimeOutMultiplier);
    private UdpClient udpClient;
    private Random randomGenerator = new Random();
    private Timer electionTimer;
    private Timer heartBeatTimer;

    public RaftNode(List<IPEndPoint> allNodes, int nodeIndex, bool reset = false)
    {
        Console.WriteLine($"RaftNode({nodeIndex})");
        Console.WriteLine($"  allNodes: {string.Join(", ", allNodes.Select(x => x.ToString()))}");
        this.allNodes = allNodes;
        this.nodeIndex = nodeIndex;
        this.udpClient = new UdpClient(allNodes[nodeIndex]);
        electionTimer = new Timer(_ => StartBeingCandidate());
        heartBeatTimer = new Timer(_ => SendHeartBeat());
        if (!reset)
        {
            LoadStateFromFiles();
        }
    }

    public Task Start()
    {
        StartBeingFollower();
        return Task.Run(HandleRequests);
    }

    public void StartBeingLeader()
    {
        Console.WriteLine("Changing to Leader");
        possiblyReplicatedEntries.Clear();
        confirmedReplicatedEntries.Clear();
        int logLength = logEntries.Count;
        for (int i = 0; i < allNodes.Count; i++)
        {
            possiblyReplicatedEntries[i] = logLength;
            if (i != nodeIndex)
            {
                confirmedReplicatedEntries[i] = 0;
            }
            else
            {
                confirmedReplicatedEntries[i] = logLength;
            }
        }
        electionTimer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        RaftState = RaftState.Leader;
        heartBeatTimer.Change(TimeSpan.Zero, Timeout.InfiniteTimeSpan); // c.f. Rules.Leaders.1
        CancelCurrentElection();
    }

    public void StartBeingFollower()
    {
        Console.WriteLine("Changing to Follower");
        RaftState = RaftState.Follower;
        RestartElectionTimer();
    }

    private void VoteForSelf()
    {
        votesForMe.Clear();
        VoteFor(nodeIndex);
        AddVote(nodeIndex);
    }

    public void StartBeingCandidate()
    {
        Console.WriteLine("Changing to Candidate");
        CancelCurrentElection();
        RaftState = RaftState.Candidate;
        IncrementTerm(); // c.f. Rules.Candidates.1.1
        VoteForSelf(); // c.f. Rules.Candidates.1.2
        RestartElectionTimer(); // c.f. Rules.Candidates.1.3

        // ask everyone for a vote // c.f. Rules.Candidates.1.4
        List<Task> tasks = new();

        foreach (int i in Enumerable.Range(0, allNodes.Count))
        {
            if (i != nodeIndex)
            {
                tasks.Add(Task.Run(() => InvokeRequestVoteRPC(i)));
            }
        }
        Task.WaitAll(tasks.ToArray());
    }

    public void HandleRequests()
    {
        while (true)
        {
            RaftMessage message = Receive(udpClient, out IPEndPoint from);
            Task.Run(() => HandleRequest(message, from));
        }
    }

    /// <summary>Updates term to most recent scene and changes to follower if not already.</summary>
    /// <returns>true if the node changes state to follower and false otherwise</returns>
    public void HandleRequest(RaftMessage message, IPEndPoint from)
    {
        switch (message.MessageType)
        {
            case MessageType.AppendEntries:
                HandleAppendEntriesRPC(message.AppendEntriesRPCInfo!, from); // c.f. Rules.Followers.1b
                break;
            case MessageType.VoteRequest:
                HandleRequestVoteRPC(message.RequestVoteRPCInfo!, from); // c.f. Rules.Followers.1a
                break;
            case MessageType.Query: // c.f. Rules.Leaders.1
                HandleQueryRPC(message, from);
                break;
            default:
                Console.WriteLine($"unknown message type {message.MessageType}.");
                break;
        }
    }

    private void HandleAppendEntriesRPC(AppendEntriesRPCInfo request, IPEndPoint from)
    {
        Console.WriteLine("Handling AppendEntries");
        RestartElectionTimer(); // c.f. Rules.Followers.2
        StepDownIfNecessary(request.LeaderTerm);
        RPCResponseInfo response = new RPCResponseInfo() { CurrentTerm = currentTerm, Response = false };
        RaftMessage responseMessage = new RaftMessage() { MessageType = MessageType.Response, RPCResponseInfo = response };
        lock (logEntries)
        {
            if (request.LeaderTerm >= currentTerm && // c.f. AppendEntries.1
                request.EntriesAlreadySent <= logEntries.Count && // c.f. AppendEntries.2a
                request.LastTermSent == logEntries[request.EntriesAlreadySent - 1].Term) // c.f. AppendEntries.2b
            {
                leaderIndex = request.LeaderIndex;
                response.Response = true;
                if (logEntries.Count > request.EntriesAlreadySent)
                {
                    // remove conflicting following messages // c.f. AppendEntries.3
                    RemoveEntries(request.EntriesAlreadySent);
                }
                AppendEntries(request.EntriesToAppend); // c.f. AppendEntries.4
                if (request.SenderSafeEntries > SafeEntries)
                {
                    SafeEntries = Math.Min(request.SenderSafeEntries, logEntries.Count); // c.f. AppendEntries.5
                    Task.Run(ApplyReadyEntries); // c.f. Rules.All.2
                }
            }
        }
        Send(responseMessage, from);
    }

    private void HandleRequestVoteRPC(RequestVoteRPCInfo request, IPEndPoint from)
    {
        Console.WriteLine("Handling VoteRequest");
        StepDownIfNecessary(request.CandidateTerm); // c.f. Rules.All.2
        RPCResponseInfo response = new RPCResponseInfo() { CurrentTerm = currentTerm, Response = false };
        RaftMessage responseMessage = new RaftMessage() { MessageType = MessageType.Response, RPCResponseInfo = response };
        int logLength = logEntries.Count;
        int lastLogTerm = logEntries[logLength - 1].Term;

        Console.WriteLine($"candidate term {request.CandidateTerm} > {currentTerm}  --- voted for {votedFor} --- candidate log term {request.CandidateLogTerm} > {lastLogTerm}");

        if (request.CandidateTerm >= currentTerm && // c.f. RequestVote.1
            (votedFor is null || votedFor == request.CandidateIndex) &&  // c.f. RequestVote.2
            request.CandidateLogLength >= logEntries.Count &&
            request.CandidateLogTerm >= lastLogTerm)
        {
            RestartElectionTimer(); // c.f. Rules.Followers.2
            response.Response = true;
            Console.WriteLine("Condition satisfied, sending back vote response");
        }
        Send(responseMessage, from);
    }

    private void HandleQueryRPC(RaftMessage message, IPEndPoint from)
    {
        Console.WriteLine("Handling Query");
        if (RaftState != RaftState.Leader) // forward to leader
        {
            Send(message, allNodes[leaderIndex]);
        }
        else
        {
            AppendEntryToLog(new LogEntry() { Term = currentTerm, Command = message.RaftCommand }); // c.f. Rules.Leaders.2a
            Task.Run(ApplyReadyEntries);
        }
    }

    private void InvokeRequestVoteRPC(int voterId)
    {
        // TODO: what if the packet gets lost
        int logLength = logEntries.Count;

        int lastLogTerm = logLength == 0 ? 0:logEntries[logLength - 1].Term;

        RequestVoteRPCInfo voteRequest = new()
        {
            CandidateTerm = currentTerm,
            CandidateIndex = nodeIndex,
            CandidateLogLength = logLength,
            CandidateLogTerm = lastLogTerm
        };
        RaftMessage reply = SendAndReceive(allNodes[voterId], new RaftMessage()
        {
            MessageType = MessageType.VoteRequest,
            RequestVoteRPCInfo = voteRequest
        });

        Console.WriteLine($"Reply for vote request {reply}");

        if (reply.RPCResponseInfo!.Response)
        {
            Console.WriteLine("Successful vote");
            AddVote(voterId);
        }
        else
        {
            UpdateTerm(reply.RPCResponseInfo.CurrentTerm);
        }
    }

    private void SendHeartBeat()
    {
        Console.WriteLine("Sending Heartbeat");
        heartBeatTimer.Change(HeartBeatTimeOut, Timeout.InfiniteTimeSpan);
        List<Task> tasks = new();
        foreach (int i in Enumerable.Range(0, allNodes.Count))
        {
            if (i != nodeIndex)
            {
                tasks.Add(Task.Run(() => SendAndReceiveOneHeartBeat(i)));
            }
        }
        Task.WaitAll(tasks.ToArray());
    }

    private void SendAndReceiveOneHeartBeat(int index)
    {
        // c.f. Rules.Leaders.2a
        while (true)
        {
            int logLength = logEntries.Count;
            int numSent = possiblyReplicatedEntries[index];
            int lastTermSent = logEntries[numSent - 1].Term;
            if (logLength > numSent)
            {
                AppendEntriesRPCInfo request = new AppendEntriesRPCInfo()
                {
                    LeaderTerm = currentTerm,
                    LeaderIndex = nodeIndex,
                    EntriesAlreadySent = numSent,
                    LastTermSent = lastTermSent,
                    EntriesToAppend = logEntries.GetRange(numSent, logLength - numSent).ToArray()
                };
                RaftMessage response = SendAndReceive(allNodes[index], new RaftMessage() { MessageType = MessageType.AppendEntries, AppendEntriesRPCInfo = request });
                if (response.RPCResponseInfo!.Response)
                {
                    possiblyReplicatedEntries[index] = logLength; // c.f. Rules.Leaders.3.1a
                    confirmedReplicatedEntries[index] = logLength; // c.f. Rules.Leaders.3.1b
                    UpdateLeaderSafeEntries();
                    Task.Run(ApplyReadyEntries); // c.f. Rules.Leaders.4
                    break;
                }
                else
                {
                    possiblyReplicatedEntries[index] = numSent - 1; // c.f. Rules.Leaders.3.2
                    continue;
                }
            }
            else
            {
                break;
            }
        }
    }

    private void UpdateLeaderSafeEntries()
    {
        var values = confirmedReplicatedEntries.Values.ToList();
        values.Sort();
        int safeCountIndex = values.Count / 2;
        SafeEntries = values[safeCountIndex];
    }

    public bool StepDownIfNecessary(int senderTerm)
    {
        if (senderTerm > currentTerm)
        {
            UpdateTerm(senderTerm);
            if (RaftState != RaftState.Follower)
            {
                StartBeingFollower(); // c.f. Rules.All.2
                return true;
            }
            else
            {
                RestartElectionTimer(); // c.f. Rules.Followers.2
            }
        }
        else if (senderTerm == currentTerm && RaftState == RaftState.Candidate)
        {
            StartBeingFollower();  // c.f. Rules.Candidates.3
        }
        return false;
    }
    private void UpdateTerm(int newTerm)
    {
        Console.WriteLine($"Updating new term {newTerm}");
        lock (currentTermPath)
        {
            currentTerm = newTerm;
            File.WriteAllText(currentTermPath, currentTerm.ToString());
        }
    }

    private void IncrementTerm()
    {
        Console.WriteLine($"Incrementing term");
        lock (currentTermPath)
        {
            currentTerm++;
            File.WriteAllText(currentTermPath, currentTerm.ToString());
        }
    }

    private void AppendEntryToLog(LogEntry entry)
    {
        Console.WriteLine($"Appending log entry {entry}");
        lock (logEntries)
        {
            logEntries.Add(entry);
            File.AppendText(entry.Json());
        }
    }

    private void VoteFor(int nodeIndex)
    {
        lock (voteLock)
        {
            votedFor = nodeIndex;
            Console.WriteLine($"{nodeIndex} Voted for {votedFor}");
            File.WriteAllText(votedForPath, "" + nodeIndex);
        }
    }

    public void RestartElectionTimer()
    {
        // c.f. Rules.Candidates.4
        Console.WriteLine("Changing election timer");
        electionTimer.Change(ElectionTimeOutRange.SampleUniform(randomGenerator), Timeout.InfiniteTimeSpan);
    }

    private void CancelCurrentElection()
    {
        // TODO: I'm not really sure how to do this
    }

    private void AppendEntries(IEnumerable<LogEntry> entries)
    {
        Console.WriteLine("Appending entries");
        AppendEntries(entries, logEntriesPath);
    }

    private void AppendEntries(IEnumerable<LogEntry> entries, string path)
    {
        File.AppendAllLines(path, entries.Select(entry => entry.Json()));
        Console.WriteLine($"Appending entry to path {path}");
        foreach (LogEntry entry in entries)
        {
            logEntries.Add(entry);
        }
    }

    private void RemoveEntries(int end)
    {
        logEntries.RemoveRange(end, logEntries.Count - end);
        // delete from file on disk; in principle this could be done more efficiently
        // delete temp file if there;
        string tempPath = $"{logEntriesPath}.tmp";
        File.CreateText(tempPath).Close();
        AppendEntries(logEntries, tempPath);
    }

    private void ApplyReadyEntries()
    {
        while (true)
        {
            LogEntry nextToApply;
            string result;
            lock (stateMachine)
            {
                if (SafeEntries > AppliedEntries)
                {
                    nextToApply = logEntries[AppliedEntries];
                    result = ApplyEntry(nextToApply);
                }
                else
                {
                    break;
                }
            }
            // send the response to the client
            if (RaftState == RaftState.Leader)
            {  // c.f. Leaders.2.b
                Task.Run(() => Send(result, nextToApply.Command!.Client!, new UdpClient()));
            }
        }
    }
    private string ApplyEntry(LogEntry logEntry)
    {
        RaftCommand command = logEntry.Command!;
        string result;
        switch (command.CommandType)
        {
            case CommandType.Get:
                result = stateMachine.Get(command.Key!).ToString();
                break;
            case CommandType.Set:
                stateMachine.Set(command.Key!, command.Value);
                result = command.Value.ToString();
                break;
            case CommandType.Init:
                // do nothing
                result = "started";
                break;
            default:
                result = "invalid";
                break;
        }
        AppliedEntries++;
        return result;
    }
    private void LoadStateFromFiles()
    {
        Console.WriteLine("Loading state from file");
        File.AppendAllText(currentTermPath, "");
        if (!int.TryParse(File.ReadAllText(currentTermPath), out currentTerm))
        {
            currentTerm = 0;
        }
        votedFor = null;
        File.AppendAllText(votedForPath, "");
        if (int.TryParse(File.ReadAllText(votedForPath), out int localVotedFor))
        {
            votedFor = localVotedFor;
        }
        logEntries.Clear();
        File.AppendAllText(logEntriesPath, "");
        foreach (string jsonLine in File.ReadAllLines(logEntriesPath))
        {
            logEntries.Add(LogEntry.FromJson(jsonLine));
        }
    }

    private static UdpClient Send(RaftMessage message, IPEndPoint otherNode)
    {
        Console.WriteLine($"Sending message to {otherNode.Address}{otherNode.Port} with message {message.MessageType}");
        return Send(message, otherNode, new UdpClient());
    }

    private static UdpClient Send(RaftMessage message, IPEndPoint otherNode, UdpClient udpClient)
    {
        Console.WriteLine($"Sending message to {otherNode.Address}{otherNode.Port} with message {message.MessageType}");
        return Send(message.Json(), otherNode, udpClient);
    }

    private static UdpClient Send(string message, IPEndPoint otherNode, UdpClient udpClient)
    {
        Console.WriteLine($"Sending message to {otherNode.Address}{otherNode.Port} with message {message}");
        byte[] data = UTF8Encoding.UTF8.GetBytes(message);
        udpClient.Send(data, data.Length, otherNode);
        return udpClient;
    }


    private static RaftMessage Receive(UdpClient udpClient)
    {
        return Receive(udpClient, out IPEndPoint _);
    }

    private static RaftMessage Receive(UdpClient udpClient, out IPEndPoint from)
    {

        IPEndPoint localFrom = new(0, 0);
        byte[] data = udpClient.Receive(ref localFrom);
        string json = UTF8Encoding.UTF8.GetString(data);
        RaftMessage returnMessage = RaftMessage.FromJson(json);
        from = localFrom;

        Console.WriteLine($"Received message{returnMessage} from {from} ");

        return returnMessage;
    }

    private static RaftMessage SendAndReceive(IPEndPoint otherNode, RaftMessage message)
    {
        var raftMessage = new RaftMessage();
        try
        {
            UdpClient myClient = Send(message, otherNode);
            raftMessage = Receive(myClient);

        }
        catch (Exception e)
        {

            Console.WriteLine(e);
        }
        return raftMessage;
    }

    private void AddVote(int voterIndex)
    {
        Console.WriteLine($"Adding a vote from {voterIndex}");
        votesForMe[voterIndex] = true;
        if (votesForMe.Count > allNodes.Count / 2) // c.f. Rules.Candidates.2
        {
            StartBeingLeader();
        }
    }
}

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

public enum MessageType
{
    VoteRequest,
    Response,
    Query,
    AppendEntries
}

record TimeSpanRange(TimeSpan Min, TimeSpan Max)
{
    public TimeSpan SampleUniform(Random randomGenerator)
    {
        double randDiff = randomGenerator.NextDouble() * ((Max - Min).TotalMilliseconds);
        return Min.Add(TimeSpan.FromMilliseconds(randDiff));
    }
}

public class LogEntry
{
    public int Term { get; set; }
    public RaftCommand? Command { get; set; }

    public static LogEntry FromJson(string json)
    {
        return JsonSerializer.Deserialize<LogEntry>(json)!;
    }
    public string Json()
    {
        return JsonSerializer.Serialize(this);
    }
}

public enum CommandType
{
    Init,
    Set,
    Get
}

/// <summary>Represents a query/update to the statemachine by a particular client (who will eventually need the response)</summary>
public class RaftCommand
{
    public CommandType CommandType { set; get; }
    public string? Key { set; get; } = null;
    public int Value { set; get; }
    public IPEndPoint? Client { get; set; }
}

