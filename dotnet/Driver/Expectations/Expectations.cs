using StreamsDB.Driver.Wire;

namespace StreamsDB.Driver.Expectations
{
    public abstract class StreamStateExpectation{
        internal abstract void InterceptRequest(Wire.AppendStreamRequest request);
    }

    internal class ExpectedVersion : StreamStateExpectation {
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
}