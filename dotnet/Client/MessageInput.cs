namespace Client
{
    public class MessageInput
    {
        public string Type { get; set; }
        public byte[] Header { get; set; }
        public byte[] Value { get; set; }
    }
}