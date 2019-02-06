using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Client;
using StreamsDB.Client;

namespace Example
{
    class Program
    {
        static void Main(string[] args)
        {
            var conn = Connection.Open("sdb://user:password@sdb03.streamsdb.io:443/example");
            var db = conn.DB();

            var input = Task.Run(async () =>
            {
                Console.WriteLine("enter a message an press [enter]");
                
                while (true)
                {
                    try
                    {
                        var line = Console.ReadLine();
                        await db.Append("stream-1", new MessageInput
                        {
                            Type = "UTF8String",
                            Value = Encoding.UTF8.GetBytes(line)
                        });
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("append failed: " + e.Message);
                    }
                }
            });

            var watch = Task.Run(async () =>
            {
                try
                {
                    var slices = db.Watch("stream-1", -1, 10);
                    var enumerator = slices.GetEnumerator();

                    while (await enumerator.MoveNext(CancellationToken.None))
                    {
                        foreach (var message in enumerator.Current.Messages)
                        {
                            var text = Encoding.UTF8.GetString(message.Value);
                            Console.WriteLine("received: " + text);
                        }
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("fatal error in watch: " + e);
                }
            });

                Task.WaitAny(input, watch);
        }
    }
}