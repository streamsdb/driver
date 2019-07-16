namespace Client
{
    public class Slice
    {
        public string Stream { get; set; }
        public long From { get; set; }
        public long Next { get; set; }
        public bool HasNext { get; set; }
        public long Head { get; set; }
        public Message[] Messages { get; set; }
    }
}
