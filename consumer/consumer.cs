using Confluent.Kafka;
using System;
using System.Threading;
using Microsoft.Extensions.Configuration;
using System.Text.Json;

class Consumer
{

    static void Main(string[] args)
    {
        /* (args.Length != 1)
        {
            Console.WriteLine("Please provide the configuration file path as a command line argument");
        }*/

        IConfiguration configuration = new ConfigurationBuilder()
            .AddIniFile(@"E:\kafka-dotnet-getting-started\getting-started.properties")
            .Build();

        configuration["group.id"] = "kafka-dotnet-getting-started";
        configuration["auto.offset.reset"] = "earliest";

        const string topic = "purchases";

        CancellationTokenSource cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) => {
            e.Cancel = true; // prevent the process from terminating.
            cts.Cancel();
        };

        using (var consumer = new ConsumerBuilder<string, string>(
            configuration.AsEnumerable()).Build())
        {
            consumer.Subscribe(topic);
            try
            {
                while (true)
                {
                    var cr = consumer.Consume(cts.Token);
                    var user = JsonSerializer.Deserialize<User>(cr.Message.Value);
                    Console.WriteLine($"Consumed event from topic {topic}: key = {cr.Message.Key} {user.FirstName} {user.LastName}");
                    
                }
            }
            catch (OperationCanceledException)
            {
                // Ctrl-C was pressed.
            }
            finally
            {
                consumer.Close();
            }
        }
    }
}