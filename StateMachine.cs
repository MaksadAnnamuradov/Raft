namespace teichert.raft;

public class StateMachine
{
    private Dictionary<string, int> values = new();

    public void Set(string key, int value) => values[key] = value;
    public int Get(string key) => values[key];

}

public interface ICommand
{
    string Execute(StateMachine stateMachine);
}

public class SetCommand : ICommand
{
    public string Key { get; init; }
    public int Value { get; init; }
    public SetCommand(string key, int value)
    {
        Key = key;
        Value = value;
    }
    public string Execute(StateMachine stateMachine)
    {
        stateMachine.Set(Key, Value);
        return "";
    }

}

public class GetCommand : ICommand
{
    public string Key { get; init; }
    public GetCommand(string key)
    {
        Key = key;
    }
    public string Execute(StateMachine stateMachine)
    {
        int result = stateMachine.Get(Key);
        return result.ToString();
    }

}
