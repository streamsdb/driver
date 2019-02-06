using System;
using System.Collections.Generic;
using System.Globalization;
using System.Reactive.Subjects;
using System.Threading;
using Grpc.Core;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using StreamsDB.Api;

namespace Streamsdb
{
    public struct PipeSliceEnumerator : IAsyncEnumerable<Slice>, IAsyncEnumerator<Slice>
    {
        private readonly IAsyncStreamReader<global::StreamsDB.Api.Slice> _source;

        public PipeSliceEnumerator(IAsyncStreamReader<StreamsDB.Api.Slice> source)
        {
            _source = source;
        }

        public Task<bool> MoveNext(CancellationToken cancellationToken) => _source.MoveNext(cancellationToken);

        public Slice Current
        {
            get
            {
                var reply = _source.Current;
                var messages = new Message[reply.Messages.Count];
                for (var i = 0; i < reply.Messages.Count; i++)
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
                    Stream = "",
                    From = reply.From,
                    To = reply.To,
                    HasNext = reply.HasNext,
                    Head = reply.Head,
                    Next = reply.Next,
                    Messages = messages,
                };
            }
        }

        public void Dispose()
        {
            _source.Dispose();
        }

        public IAsyncEnumerator<Slice> GetEnumerator()
        {
            return this;
        }
    }

    public class DB
    {
        private readonly StreamsDB.Api.StreamsDB.StreamsDBClient _client;
        private readonly string _token;
        private readonly string _db;

        public DB(StreamsDB.Api.StreamsDB.StreamsDBClient client, string token, string db)
        {
            _client = client;
            _token = token;
            _db = db;
        }

        public async Task<long> Append(string streamId, params MessageInput[] messages)
        {
            var reply = await _client.AppendAsync(new AppendRequest
            {
                Database = _db,
                Stream = streamId,
            }, new Metadata()
            {
                {"token", _token},
            });

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
            }, new Metadata()
            {
                {"token", _token},
            }, cancellationToken: cancellationToken);

            return new PipeSliceEnumerator(watch.ResponseStream);
        }

        public async Task<Slice> Read(string streamId, long from, int count)
        {
            var reply = await _client.ReadAsync(new ReadRequest
            {
                Database = _db,
                Stream = streamId,
                From = from,
                Count = (uint) count,
            }, new Metadata()
            {
                {"token", _token},
            });

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

    public class MessageInput
    {
        public string Type { get; set; }
        public byte[] Metadata { get; set; }
        public byte[] Value { get; set; }
    }

    public class Message
    {
        public string Type { get; set; }
        public Timestamp Timestamp { get; set; }
        public byte[] Metadata { get; set; }
        public byte[] Value { get; set; }
    }

    public class Slice
    {
        public string Stream { get; set; }
        public long From { get; set; }
        public long To { get; set; }
        public long Next { get; set; }
        public bool HasNext { get; set; }
        public long Head { get; set; }
        public Message[] Messages { get; set; }
    }

    public class Connection
    {
        private readonly StreamsDB.Api.StreamsDB.StreamsDBClient _client;
        private string _token;

        private Connection(StreamsDB.Api.StreamsDB.StreamsDBClient client)
        {
            _client = client;
        }

        public void SetToken(string token)
        {
            _token = token;
        }

        public Connection Open(string connectionString)
        {
            var uri = new Uri(connectionString);

            Channel channel = new Channel(connectionString, new SslCredentials());
            var client = new StreamsDB.Api.StreamsDB.StreamsDBClient(channel);
            return new Connection(client);
        }

        public async void Login(string username, string password)
        {
            var reply = await _client.LoginAsync(new LoginRequest {Username = username, Password = password,});
            _token = reply.Token;
        }

        public DB DB(string db)
        {
            return new DB(_client, _token, db);
        }
    }
}