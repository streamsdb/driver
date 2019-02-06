using System;
using System.Web;
using Grpc.Core;
using Streamsdb.Wire;
using WireClient = Streamsdb.Wire.Streams.StreamsClient;

namespace StreamsDB.Client
{
    public class Connection
    {
        private readonly Channel _channel;
        private readonly WireClient _client;
        private volatile string _db;
        private readonly Metadata _metadata = new Metadata();

        private Connection(Channel channel, WireClient client, string defaultDb = null)
        {
            _channel = channel;
            _client = client;
            _db = defaultDb;
        }

        public static Connection Open(string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new ArgumentNullException(nameof(connectionString));
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
            var client = new WireClient(channel);
            String defaultDb = null;
            if (!string.IsNullOrEmpty(uri.AbsolutePath))
            {
                defaultDb = uri.AbsolutePath.Trim('/');
            }
            
            var conn = new Connection(channel, client, defaultDb);
            if(!string.IsNullOrEmpty(uri.UserInfo))
            {
                var items = uri.UserInfo.Split(new char[] {':'});
                var username = items[0];
                var password = items[1];

                conn.Login(username, password);
            }
            
            return conn;
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