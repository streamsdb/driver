using System;
using System.Web;
using Grpc.Core;
using StreamsDB.Driver.Wire;
using static StreamsDB.Driver.Wire.Streams;

namespace StreamsDB.Driver
{
    public class StreamsDBClient
    {
        private readonly Channel _channel;
        private readonly StreamsClient _client;
        private volatile string _db;
        private readonly Metadata _metadata = new Metadata();

        public StreamsDBClient(string connectionString = null)
        {
            if (string.IsNullOrEmpty(connectionString)) {
              connectionString = Environment.GetEnvironmentVariable("SDB_HOST");
            }

            if (string.IsNullOrEmpty(connectionString)) {
              throw new ArgumentNullException(nameof(connectionString), "connection string not specified and SDB_HOST environment variable is empty");
            }
            
            if (!connectionString.StartsWith("sdb://"))
            {
                throw new ArgumentOutOfRangeException(nameof(connectionString), "invalid streamsdb connection string: not starting with 'sdb://'");
            }
            
            var uri = new Uri(connectionString);

            var options = HttpUtility.ParseQueryString(uri.Query);
            ChannelCredentials cred = new SslCredentials();

            var value = options.Get("insecure");
            if (value != null && value == "1")
            {
                cred = ChannelCredentials.Insecure;
            }

            var channel = new Channel(uri.Host, uri.Port, cred);
            var apiClient = new StreamsClient(channel);
            String defaultDb = null;
            if (!string.IsNullOrEmpty(uri.AbsolutePath))
            {
                defaultDb = uri.AbsolutePath.Trim('/');
            }
            
            _channel = channel;
            _client = apiClient;
            _db = defaultDb;

            if(!string.IsNullOrEmpty(uri.UserInfo))
            {
                var items = uri.UserInfo.Split(new char[] {':'});
                var username = HttpUtility.UrlDecode(items[0]);
                var password = HttpUtility.UrlDecode(items[1]);

                this.Login(username, password);
            }
        }

        public void Login(string username, string password)
        {
            var reply = _client.Login(new LoginRequest {Username = username, Password = password,});
            _metadata.Add("token", reply.Token);
        }

        public DB DB(string db = null)
        {
            if (string.IsNullOrEmpty(db))
            {
                if (string.IsNullOrEmpty(_db))
                {
                    throw new ArgumentNullException(nameof(db));
                }
                db = _db;
            }
            return new DB(_client, db, _metadata);
        }

        public async void Close()
        {
            await _channel.ShutdownAsync();
        }
    }
}
