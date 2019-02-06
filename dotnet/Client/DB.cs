using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using Streamsdb.Wire;
using Message = Client.Message;
using MessageInput = Client.MessageInput;
using Slice = Client.Slice;
using WireClient = Streamsdb.Wire.Streams.StreamsClient;
using WireMessageInput = Streamsdb.Wire.MessageInput;

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
            var request = new AppendRequest
            {
                Database = _db,
                Stream = streamId,
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
                    Metadata = ByteString.CopyFrom(m.Metadata ?? new byte[0]),
                    Value = ByteString.CopyFrom(m.Value ?? new byte[0]),
                });
            }

            var reply = await _client.AppendAsync(request, _metadata);

            return reply.From;
        }

        public IAsyncEnumerable<Slice> Watch(string streamId, long from, int count,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var watch = _client.Watch(new ReadRequest
            {
                Database = _db,
                Stream = streamId,
                From = from,
                Count = (uint) count,
            },_metadata, cancellationToken: cancellationToken);

            return new PipeSliceEnumerator(streamId, watch.ResponseStream);
        }

        public async Task<Slice> Read(string streamId, long from, int count)
        {
            var reply = await _client.ReadAsync(new ReadRequest
            {
                Database = _db,
                Stream = streamId,
                From = from,
                Count = (uint) count,
            }, _metadata);

            var messages = new Message[reply.Messages.Count];
            for (int i = 0; i < reply.Messages.Count; i++)
            {
                var am = reply.Messages[i];

                messages[i] = new Message
                {
                    Type = am.Type,
                    Timestamp = am.Timestamp,
                    Metadata = am.Metadata.ToByteArray(),
                    Value = am.Value.ToByteArray(),
                };
            }

            return new Slice
            {
                Stream = streamId,
                From = reply.From,
                To = reply.To,
                HasNext = reply.HasNext,
                Head = reply.Head,
                Next = reply.Next,
                Messages = messages,
            };
        }
    }
}