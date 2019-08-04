using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using StreamsDB.Driver.Wire;
using static StreamsDB.Driver.Wire.Streams;
using Message = Client.Message;
using MessageInput = Client.MessageInput;
using Slice = Client.Slice;

namespace StreamsDB.Driver
{
    public abstract class ConcurrencyCheck{
        internal abstract void InterceptRequest(Wire.AppendStreamRequest request);

        public static ConcurrencyCheck Skip() => new ExpectedVersion(-2);

        public static ConcurrencyCheck ExpectVersion(long version) => new ExpectedVersion(version);

        public static ConcurrencyCheck ExpectLastMessage(Client.Message message) => new ExpectedVersion(message.Position);
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

        public async Task<(Client.Message, bool)> ReadMessageFromStream(string streamId, long position) {
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
        public async Task<long> AppendStream(string streamId, ConcurrencyCheck guard, params MessageInput[] messages) => await AppendStream(streamId, guard, (IEnumerable<MessageInput>)messages);

        /// <summary>
        /// AppendStream appends the provides messages to the specified stream.
        /// </summary>
        /// <param name="streamId">The stream to append to. If the stream does not exists, it will be created.</param>
        /// <param name="expectation">The stream state expection for optimistic concurrency. Use <c>Expect.Nothing()</c> or <c>Expect.HeadAt()</c>.</param>
        /// <param name="messages">The messages to append.</param>
        /// <returns>The position in the stream of the first message that has been written.</returns>
        /// 
        public async Task<long> AppendStream(string streamId, ConcurrencyCheck guard, IEnumerable<MessageInput> messages)
        {
            var request = new AppendStreamRequest
            {
                Database = _db,
                Stream = streamId,
                ExpectedVersion = -2,
            };

            guard.InterceptRequest(request);

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

        public IAsyncEnumerator<Message> SubscribeStream(string streamId, long from, int count,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var watch = _client.SubscribeStream(new SubscribeStreamRequest
            {
                Database = _db,
                Stream = streamId,
                From = from,
                Count = (uint) count,
            },_metadata, cancellationToken: cancellationToken);

            return new StreamSubscription(streamId, watch.ResponseStream);
        }

        public async Task<Slice> ReadStreamForward(string streamId, long from, int limit) => await read(streamId, from, false, limit);

        public async Task<Slice> ReadStreamBackward(string streamId, long from, int limit) => await read(streamId, from, true, limit);

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
