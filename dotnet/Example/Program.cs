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
            var conn = Connection.Open("sdb://sdb-01.streamsdb.io:443/default");
            var db = conn.DB();
            var streamName = "chat";

            // read user input and append it to the stream
            var input = Task.Run(async () =>
            {
                Console.WriteLine("enter a message an press [enter]");
                
                while (true)
                {
                    try
                    {
                        var line = Console.ReadLine();
                        await db.Append(streamName, new MessageInput
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

            var read = Task.Run(async () =>
            {
                try
                {
                    var slices = db.Subscribe(streamName, -1, 10);
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
                    Console.WriteLine("read error: " + e);
                }
            });

            Task.WaitAny(input, read);
        }
    }
}
