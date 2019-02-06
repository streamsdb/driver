using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Client;
using Grpc.Core;

namespace StreamsDB.Client
{
    internal struct PipeSliceEnumerator : IAsyncEnumerable<Slice>, IAsyncEnumerator<Slice>
    {
        private readonly string _streamId;
        private readonly IAsyncStreamReader<Streamsdb.Wire.Slice> _source;

        public PipeSliceEnumerator(string streamId, IAsyncStreamReader<Streamsdb.Wire.Slice> source)
        {
            _streamId = streamId;
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
                    Stream = _streamId,
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
}