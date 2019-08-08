namespace StreamsDB.Driver
{
    /// <summary>
    /// Represents a message that can be written to a stream.
    /// </summary>
    public class MessageInput
    {
        /// <summary>
        /// Gets or sets the ID of the message.
        /// </summary>
        /// <value>A string value representing the ID of the message.</value>
        public string ID {get; set;}

        /// <summary>
        /// Gets or sets the type of the message.
        /// </summary>
        /// <value>A string value representing the type of the message.</value>
        public string Type { get; set; }

        /// <summary>
        /// Gets or sets the header of the message.
        /// </summary>
        /// <value>A byte array.</value>
        public byte[] Header { get; set; }

        /// <summary>
        /// Gets or sets the value of the message.
        /// </summary>
        /// <value>A byte array.</value>
        public byte[] Value { get; set; }
    }
}