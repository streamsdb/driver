using System;

namespace StreamsDB.Driver
{
    /// <summary>
    /// Represents a message on a stream.
    /// </summary>
    public class Message
    {
        /// <summary>
		/// The position of this message in the stream.
		/// </summary>
        public long Position {get; set; }
        
        /// <summary>
		/// The type of this message.
		/// </summary>
        public string Type { get; set; }

        /// <summary>
        /// The point in time this message was created.
        /// </summary>
        public DateTime Timestamp { get; set; }

        /// <summary>
        /// The header of this message in bytes.
        /// </summary>
        public byte[] Header { get; set; }

        /// <summary>
        /// The value of this message in bytes.
        /// </summary>
        public byte[] Value { get; set; }
    }
}