namespace Client
{
    public class MessageInput
    {
        public string ID {get; set;}
        public string Type { get; set; }
        public byte[] Header { get; set; }
        public byte[] Value { get; set; }
    }
}