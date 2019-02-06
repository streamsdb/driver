using Google.Protobuf.WellKnownTypes;

namespace Client
{
    public class Message
    {
        public string Type { get; set; }
        public Timestamp Timestamp { get; set; }
        public byte[] Metadata { get; set; }
        public byte[] Value { get; set; }
    }
}