namespace StreamsDB.Driver
{
    public interface IGlobalSlice {
        GlobalPosition From {get;}

        GlobalPosition Next {get;}

        Message[] Messages {get;}

        bool HasNext {get;}
    }

    public class GlobalSlice : IGlobalSlice {
        public GlobalPosition From {get; set;}
        public GlobalPosition Next {get; set;}

        public Message[] Messages {get;set;}

        public bool HasNext 
        {
            get
            {
                return !From.Equals(Next);
            }
        }
    }

    /// <summary>
    /// Represents a slice of a stream that hold messages from a certain position in a stream.
    /// </summary>
    public interface IStreamSlice {
        /// <summary>
        /// Gets the name of the stream.
        /// </summary>
        string Stream {get;}

        /// <summary>
        /// Gets the from position of this slice.
        /// </summary>
        long From {get;}

        /// <summary>
        /// Gets an indication if there are more messages availble at the time of reading.
        /// </summary>
        /// <value></value>
        bool HasNext{get;}

        /// <summary>
        /// Gets the next position to continue reading.
        /// </summary>
        long Next {get;}

        /// <summary>
        /// Gets the messages.
        /// </summary>
        Message[] Messages {get;}

        /// <summary>
        /// Gets an indication whether this slice is in reverse direction.
        /// </summary>
        bool Reverse {get;}
    }

    internal class Slice : IStreamSlice
    {
        public string Stream { get; set; }

        public long From { get; set; }

        public long Next { get; set; }

        public bool HasNext { get; set; }
        public long Head { get; set; }
        public Message[] Messages { get; set; }
        public bool Reverse {get; set; }
    }
}
