using System;
using System.Text;
using System.Threading.Tasks;
using StreamsDB.Driver;

namespace StreamsDB.Example
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var client = await StreamsDBClient.Connect();
            var db = client.DB();

            // read from stdin and write to stream
            var input = Task.Run(async () =>
            {
                Console.WriteLine("enter a message an press [enter]");
                
                while (true)
                {
                    try
                    {
                        var line = Console.ReadLine();
                        await db.AppendStream("chat", new MessageInput
                        {
                            Type = "string",
                            Value = Encoding.UTF8.GetBytes(line)
                        });
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("append failed: " + e.Message);
                    }
                }
            });

            // subscribe to stream and print messages
            var read = Task.Run(async () =>
            {
                try
                {
					await using var subscription = db.SubscribeStream("chat", -1);

					while (await subscription.MoveNextAsync())
					{
						var message = subscription.Current;

						var text = Encoding.UTF8.GetString(message.Value);
						Console.WriteLine("received: " + text);
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
