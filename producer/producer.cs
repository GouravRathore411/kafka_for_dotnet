using Confluent.Kafka;
using System;
using Microsoft.Extensions.Configuration;
using System.Text.Json;


class Producer
{
    static void Main(string[] args)
    {
        /*f (args.Length != 1) {
             Console.WriteLine("Please provide the configuration file path as a command line argument");
         }*/

        IConfiguration configuration = new ConfigurationBuilder()
            .AddIniFile(@"E:\kafka-dotnet-getting-started\getting-started.properties")
            .Build();

        const string topic = "purchases";

        string[] users = { "eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther" };
        string[] items = { "bookd", "alarm clock", "t-shirts", "gift card", "batteries" };

        using (var producer = new ProducerBuilder<string, string>(
            configuration.AsEnumerable()).Build())
        {
            var numProduced = 0;
            var keyvalue = 0;
            //Random rnd = new Random();
            //const int numMessages = 10;
            int count = 0;
            while(count <= 100)
            {
                var user = new User()
                {
                    FirstName = Faker.Name.First(),
                    LastName = Faker.Name.Last()
                };

                producer.Produce(topic, new Message<string, string> { Key = 
                    keyvalue++.ToString(), Value = JsonSerializer.Serialize(user)
                },
                    (deliveryReport) =>
                    {
                        if (deliveryReport.Error.Code != ErrorCode.NoError)
                        {
                            Console.WriteLine($"Failed to deliver message: {deliveryReport.Error.Reason}");
                        }
                        else
                        {
                            Console.WriteLine($"Produced event to topic {topic}: key = {keyvalue} value ={user.FirstName} {user.LastName}");
                            numProduced += 1;
                        }
                    });
                count++;
            }

            producer.Flush(TimeSpan.FromSeconds(10));
            Console.WriteLine($"{numProduced} messages were produced to topic {topic}");
        }
    }
}

 