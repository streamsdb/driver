using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;

namespace StreamsDB.Driver
{
    internal class StreamSubscription : IStreamSubscription
    {
        private readonly string _streamId;
        private readonly IAsyncStreamReader<StreamsDB.Driver.Wire.Slice> _source;

        private IEnumerator<Message> _currentEnumerator = Enumerable.Empty<Message>().GetEnumerator();

        private Message _currentValue;

        private int _currentIndex = -1;

        public StreamSubscription(string streamId, IAsyncStreamReader<StreamsDB.Driver.Wire.Slice> source)
        {
            _streamId = streamId;
            _source = source;
        }

        public async Task<bool> MoveNext(CancellationToken cancellationToken) {
            if(!_currentEnumerator.MoveNext()) {
                // loop till we have no empty slice
                while(true)
                {
                    if(!await _source.MoveNext(cancellationToken)) {
                        return false;
                    }

                    var slice = _source.Current;
                    if(slice.Messages.Count == 0) {
                        continue;
                    }

                    _currentEnumerator = _source.Current.Messages.Select(m => new Message{
                        Stream = m.Stream,
                        Position = m.Position,
                        Type = m.Type,
                        Timestamp = m.Timestamp.ToDateTime(),
                        Header = m.Header.ToByteArray(),
                        Value = m.Value.ToByteArray(),
                    }).GetEnumerator();

                    return await MoveNext(cancellationToken);
                }
            }
            return true;
        }

        public Message Current => _currentEnumerator.Current;

        public void Dispose()
        {
            _source.Dispose();
        }
    }
}
