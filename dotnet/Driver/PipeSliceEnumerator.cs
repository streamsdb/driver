using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Grpc.Core;

namespace StreamsDB.Driver
{
    internal class StreamSubscription : IStreamSubscription
    {
        private readonly IAsyncStreamReader<Wire.Slice> _source;

        private IEnumerator<Message> _currentEnumerator = Enumerable.Empty<Message>().GetEnumerator();

        public StreamSubscription(IAsyncStreamReader<Wire.Slice> source)
        {
            _source = source;
        }

        public async ValueTask<bool> MoveNextAsync() {
            if(!_currentEnumerator.MoveNext()) {
                // loop till we have no empty slice
                while(true)
                {
                    if(!await _source.MoveNext()) {
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

                    return await MoveNextAsync();
                }
            }
            return true;
        }

		public ValueTask DisposeAsync()
		{
			return new ValueTask();
		}

		public Message Current => _currentEnumerator.Current;
    }
}
