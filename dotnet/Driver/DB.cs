using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using StreamsDB.Driver.Wire;
using static StreamsDB.Driver.Wire.Streams;

namespace StreamsDB.Driver
{
    /// <summary>
    /// IStreamSubscription represents a subscription to a stream that can be used
    /// to get current and future messages from a stream.
    /// <seealso cref="DB.SubscribeStream" />
    /// </summary>
    public interface IStreamSubscription : IAsyncEnumerator<Message>{}

    /// <summary>
    /// ConcurrencyCheck specifies the optimistic concurrency check that needs to succeed before
    /// a write transaction gets committed. If the check reveals conflicting modifications, the
    /// write transaction is aborted.
    /// 
    /// Use <see cref="Skip" /> to disable the optimistic concurrency check, or use one of the other
    /// static methods of this class to specify a check.
    /// </summary>
    /// <example>
    /// Here is an example that writes a strict monotonically increasing number to a stream. 
    /// Because of the <see cref="ConcurrencyCheck.ExpectStreamLastMessage" /> this 
    /// example could be run concurrently and the numbers on the steam will still be monotonicly increasing:
    /// <code>
    /// int nextNumber;
    /// ConcurrencyCheck check;
    /// 
    /// while (true) {
    ///   // read the last message from the stream
    ///   var (message, found) = await db.ReadMessageFromStream("exact-sequence", -1);
    /// 
    ///   if (found)
    ///   {
    ///     // get the number from the value of the last message and increase
    ///     nextNumber = BitConverter.ToInt32(message.Value) + 1;
    /// 
    ///     // expect the message we read to be the last message on the stream
    ///     check = ConcurrencyCheck.ExpectLastMessage(message);
    ///   }
    ///   else
    ///   {
    ///     nextNumber = 0;
    ///     check = ConcurrencyCheck.ExpectVersion(0);
    ///   }
    /// 
    ///   try {
    ///     await db.AppendStream("exact-sequence", check, new MessageInput
    ///     {
    ///       Type = "int32",
    ///       Value = BitConverter.GetBytes(nextNumber)
    ///     });
    ///   } catch(OperationAbortedException caught) {
    ///     // The operation was aborted, typically due to
    ///     // a concurrency issue such as a concurrency check failure.
    ///     continue;
    ///   }
    /// }
    /// </code>
    /// </example>
    public abstract class ConcurrencyCheck{
        internal abstract void InterceptRequest(Wire.AppendStreamRequest request);

        /// <summary>
        /// Skip returns an optimistic concurrency check that will always succeed. In other words, the optimistic
        /// concurrency control will be disabled for the write transaction where this check is used.
        /// </summary>
        /// <returns>An optimistic concurrency control check that will always succeed.</returns>
        public static ConcurrencyCheck Skip() => new ExpectedVersion(-2);

        /// <summary>
        /// ExpectStreamNotExists returns an optimistic concurrency check that will only succeed if the stream does not
        /// exist.
        /// </summary>
        /// <returns>An optimistic concurrency control check that will only succeed if the stream does not exist.</returns>
        public static ConcurrencyCheck ExpectStreamNotExists() => new ExpectedVersion(0);

        /// <summary>
        /// Expect the stream to have the specified version.
        /// </summary>
        /// <param name="version">The expected version of the stream.</param>
        /// <returns>An optimistic concurrency control check that will only succeed if the stream has the specified version.</returns>
        public static ConcurrencyCheck ExpectStreamVersion(long version) => new ExpectedVersion(version);

        /// <summary>
        /// Expect the stream to have the version that is equal to the position of the specified messaage.
        /// </summary>
        /// <remarks>
        /// It's important to understand that only the position will be verfied, not the actual value of the message nor it's ID or headers.
        /// </remarks>
        /// <param name="message">The message to use in the check.</param>
        /// <returns>An optimistic concurrency control check that will only succeed if the stream version is equal to the position of the specified message.</returns>
        public static ConcurrencyCheck ExpectStreamLastMessage(Message message) => new ExpectedVersion(message.Position);
    }

    internal class ExpectedVersion : ConcurrencyCheck {
        private readonly long _version;

        public ExpectedVersion(long version){
            _version = version;
        }

        public long ToVersionIdentifier()
        {
            return _version;
        }

        internal override void InterceptRequest(AppendStreamRequest request)
        {
            request.ExpectedVersion = _version;
        }
    }

    /// <summary>
    /// DB represents a database in StreamsDB.
    /// </summary>
    public class DB
    {
        private readonly StreamsClient _client;
        private readonly string _db;
        private readonly Metadata _metadata = Metadata.Empty;

        internal DB(StreamsClient client, string db, Metadata metadata)
        {
            _client = client;
            _db = db;
            _metadata = metadata;
        }

        /// <summary>
        /// AppendStream appends the provides messages to the specified stream.
        /// </summary>
        /// <param name="streamId">The stream to append to. If the stream does not exists, it will be created.</param>
        /// <param name="messages">The messages to append.</param>
        /// <returns>The position in the stream of the first message that has been written.</returns>
        public async Task<long> AppendStream(string streamId, IEnumerable<MessageInput> messages) => await AppendStream(streamId, ConcurrencyCheck.Skip(), messages);

        /// <summary>
        /// AppendStream appends the provides messages to the specified stream.
        /// </summary>
        /// <param name="streamId">The stream to append to. If the stream does not exists, it will be created.</param>
        /// <param name="messages">The messages to append.</param>
        /// <returns>The position in the stream of the first message that has been written.</returns>
        public async Task<long> AppendStream(string streamId, params MessageInput[] messages) => await AppendStream(streamId, ConcurrencyCheck.Skip(), messages);

        /// <summary>
        /// ReadLastMessageFromStream returns the last message of a stream.
        /// </summary>
        /// <returns>A tuple containing the message and a boolean indication whether the message was found or not.</returns>
        public async Task<(Message, bool)> ReadLastMessageFromStream(string streamId) {
            return await ReadMessageFromStream(streamId, -1);
        }

        /// <summary>
        /// ReadMessageFromStream returns the message from the stream at the specified position.
        /// </summary>
        /// <returns>A tuple containing the message and a boolean indication whether the message was found or not.</returns>
        public async Task<(Message, bool)> ReadMessageFromStream(string streamId, long position) {
            try
            {
                var slice =  await ReadStreamForward(streamId, position, 1);
                if(slice.Messages.Length == 0) {
                    return (default(Message), false);
                }

                return (slice.Messages[0], true);
            } catch(RpcException caught) {
                if(caught.Status.Detail == "stream does not exist") {
                    return (default(Message), false);
                }

                throw caught;
            }
        }


        /// <summary>
        /// AppendStream appends the provides messages to the specified stream.
        /// </summary>
        /// <param name="streamId">The stream to append to. If the stream does not exists, it will be created.</param>
        /// <param name="concurrencyCheck">The optimistic concurrency check. See <see cref="ConcurrencyCheck"/> for different options.</param>
        /// <param name="messages">The messages to append.</param>
        /// <returns>The position in the stream of the first message that has been written.</returns>        
        public async Task<long> AppendStream(string streamId, ConcurrencyCheck concurrencyCheck, params MessageInput[] messages) => await AppendStream(streamId, concurrencyCheck, (IEnumerable<MessageInput>)messages);

        /// <summary>
        /// AppendStream appends the provides messages to the specified stream.
        /// </summary>
        /// <param name="streamId">The stream to append to. If the stream does not exists, it will be created.</param>
        /// <param name="concurrencyCheck">The optimistic concurrency check. See <see cref="ConcurrencyCheck"/> for different options.</param>
        /// <param name="messages">The messages to append.</param>
        /// <returns>The position in the stream of the first message that has been written.</returns>
        public async Task<long> AppendStream(string streamId, ConcurrencyCheck concurrencyCheck, IEnumerable<MessageInput> messages)
        {
            var request = new AppendStreamRequest
            {
                Database = _db,
                Stream = streamId,
                ExpectedVersion = -2,
            };

            concurrencyCheck.InterceptRequest(request);

            foreach(var m in messages)
            {
                if (string.IsNullOrEmpty(m.Type))
                {
                    throw new ArgumentNullException(nameof(m.Type), "missing type name");
                }
                
                request.Messages.Add(new Wire.MessageInput
                {
                    Type = m.Type,
                    Header = ByteString.CopyFrom(m.Header ?? new byte[0]),
                    Value = ByteString.CopyFrom(m.Value ?? new byte[0]),
                });
            }

            var reply = await _client.AppendStreamAsync(request, _metadata);
            return reply.From;
        }

        /// <summary>
        /// SubscribeStream creates a stream subscription that allows you to read from a stream and receive future writes.
        /// </summary>
        /// <param name="streamId">The stream to subscribe to.</param>
        /// <param name="from">The position to subscribe from.</param>
        /// <param name="cancellationToken">The cancellation token to cancel the subscription.</param>
        /// <returns>A stream subscription.</returns>
        public IStreamSubscription SubscribeStream(string streamId, long from, CancellationToken cancellationToken = default(CancellationToken))
        {
            var watch = _client.SubscribeStream(new SubscribeStreamRequest
            {
                Database = _db,
                Stream = streamId,
                From = from,
                Count = (uint) 10, // TODO: allow specification of slice size
            },_metadata, cancellationToken: cancellationToken);

            return new StreamSubscription(streamId, watch.ResponseStream);
        }

        /// <summary>
        /// ReadStreamForward reads from a stream in the forward direction, in other words, reading from older to newer messages in a stream.
        /// </summary>
        /// <param name="streamId">The stream to read from.</param>
        /// <param name="from">The position to read from.</param>
        /// <param name="limit">The maximum number of messages to read.</param>
        /// <returns>A stream slice.</returns>
        public async Task<IStreamSlice> ReadStreamForward(string streamId, long from, int limit) => await read(streamId, from, false, limit);

        /// <summary>
        /// ReadStreamBackward reads from a stream in the backward direction, in other words, reading from newer to older messages in the stream.
        /// </summary>
        /// <param name="streamId">The stream to read from.</param>
        /// <param name="from">The position to read from.</param>
        /// <param name="limit">The maximum number of messages to read.</param>
        /// <returns>A stream slice.</returns>
        public async Task<IStreamSlice> ReadStreamBackward(string streamId, long from, int limit) => await read(streamId, from, true, limit);

        private async Task<Slice> read(string streamId, long from, bool reverse, int limit)
        {
            var reply = await _client.ReadStreamAsync(new ReadStreamRequest
            {
                Database = _db,
                Stream = streamId,
                From = from,
                Limit = (uint) limit,
                Reverse = reverse,
            }, _metadata);

            var messages = new Message[reply.Messages.Count];
            for (int i = 0; i < reply.Messages.Count; i++)
            {
                var am = reply.Messages[i];

                messages[i] = new Message
                {
                    Position = am.Position,
                    Type = am.Type,
                    Timestamp = am.Timestamp.ToDateTime(),
                    Header = am.Header.ToByteArray(),
                    Value = am.Value.ToByteArray(),
                };
            }

            return new Slice
            {
                Stream = streamId,
                From = reply.From,
                HasNext = reply.HasNext,
                Head = reply.Head,
                Next = reply.Next,
                Messages = messages,
                Reverse = reply.Reverse,
            };
        }
    }
}
