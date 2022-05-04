using System.Text.Json;
namespace teichert.raft;

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

