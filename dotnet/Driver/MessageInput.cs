using System.Collections.Generic;

namespace StreamsDB.Driver
{
    /// <summary>
    /// Represents input for a atomic stream write.
    /// </summary>
    public class StreamInput {
      public string Stream{get;}
      public ConcurrencyCheck ConcurrencyCheck {get;}
      public IEnumerable<MessageInput> Messages {get;}

      public StreamInput(string stream, ConcurrencyCheck concurrencyCheck, IEnumerable<MessageInput> messages)
      {
          Stream = stream;
          ConcurrencyCheck = concurrencyCheck;
          Messages = messages;
      }
    }

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
