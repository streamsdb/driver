using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using StreamsDB.Wire;
using Message = Client.Message;
using MessageInput = Client.MessageInput;
using Slice = Client.Slice;
using WireClient = StreamsDB.Wire.Streams.StreamsClient;
using WireMessageInput = StreamsDB.Wire.MessageInput;

namespace StreamsDB.Client
{
    public class DB
    {
        private readonly WireClient _client;
        private readonly string _db;
        private readonly Metadata _metadata = Metadata.Empty;

        public DB(WireClient client, string db, Metadata metadata)
        {
            _client = client;
            _db = db;
            _metadata = metadata;
        }

        public async Task<long> Append(string streamId, params MessageInput[] messages)
        {
            var request = new AppendStreamRequest
            {
                Database = _db,
                Stream = streamId,
                ExpectedVersion = -2,
            };
            

            foreach(var m in messages)
            {
                if (string.IsNullOrEmpty(m.Type))
                {
                    throw new ArgumentNullException(nameof(m.Type), "missing type name");
                }
                
                request.Messages.Add(new WireMessageInput
                {
                    Type = m.Type,
                    Header = ByteString.CopyFrom(m.Header ?? new byte[0]),
                    Value = ByteString.CopyFrom(m.Value ?? new byte[0]),
                });
            }

            var reply = await _client.AppendStreamAsync(request, _metadata);

            return reply.From;
        }

        public IAsyncEnumerable<Slice> Subscribe(string streamId, long from, int count,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var watch = _client.SubscribeStream(new SubscribeStreamRequest
            {
                Database = _db,
                Stream = streamId,
                From = from,
                Count = (uint) count,
            },_metadata, cancellationToken: cancellationToken);

            return new PipeSliceEnumerator(streamId, watch.ResponseStream);
        }

        public async Task<Slice> ReadForward(string streamId, long from, int limit) => await read(streamId, from, false, limit);

        public async Task<Slice> ReadBackward(string streamId, long from, int limit) => await read(streamId, from, true, limit);

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
