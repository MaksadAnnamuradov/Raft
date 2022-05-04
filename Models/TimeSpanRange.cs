namespace teichert.raft;

record TimeSpanRange(TimeSpan Min, TimeSpan Max)
{
    public TimeSpan SampleUniform(Random randomGenerator)
    {
        double randDiff = randomGenerator.NextDouble() * ((Max - Min).TotalMilliseconds);
        return Min.Add(TimeSpan.FromMilliseconds(randDiff));
    }
}

