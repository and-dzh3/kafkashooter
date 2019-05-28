using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;

//this tiny tool for kafka load and performance testing (producer)
//build and run good luck
namespace perfkafka
{
    class Program
    {

        static void Main(string[] args)
        {
            int load = 25;                    //hom much topics to load
            int massagesCount = 1000001;      //messages to load
            string msg = stringGenerator(1024);       //1kb msg size 
            string brokerList = "localhost:9092";                            //kafka addres

            var config = new ProducerConfig
            {
                BootstrapServers = brokerList,
                BrokerVersionFallback = "0.10.0.0",
                ApiVersionFallbackMs = 0,
                // SaslMechanism = SaslMechanism.Plain,                      // uncomment for authentication 
                // SecurityProtocol = SecurityProtocol.SaslPlaintext,
                // SaslUsername = "admin",
                // SaslPassword = "admin",
                Acks = Acks.Leader,
                QueueBufferingMaxMessages = 1000
                // Debug="All" //uncomment for debug
            };

            using (var p = new ProducerBuilder<string, string>(config).Build())
            {
                List<string> topicslist = GenerateTopicsName(load);

                var result = Parallel.ForEach(topicslist, (topic) =>
                {
                    int failed = 0;
                    int succeed = 0;
                    Stopwatch sw = new Stopwatch();
                    sw.Start();
                    try
                    {
                        for (int i = 1; i < massagesCount; i++)
                        {
                            var deliveryReport = p.ProduceAsync(topic, new Message<string, string> { Key = $"{i}", Value = msg }).ContinueWith(task => task.IsFaulted ? $"error producing message: {task.Exception.Message}" : $"produced to: {task.Result.TopicPartitionOffset}");
                            if (deliveryReport.IsFaulted)
                            {
                                failed++;
                            }

                            else
                            {
                                succeed++;
                            }
                        }
                        p.Flush(TimeSpan.FromSeconds(4));
                    }
                    catch (ProduceException<Null, string> e)
                    {
                        Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                    }
                    p.Flush(TimeSpan.FromSeconds(10));
                    sw.Stop();
                    Console.WriteLine($"-----Elapsed={sw.Elapsed} {topic.ToUpper()} -----");
                    Console.WriteLine($"Succeeded : {succeed}  , Failed : {failed}");
                    Console.WriteLine($"Msg/sec : {massagesCount / sw.Elapsed.TotalSeconds}");
                });

            }
        }

        private static List<string> GenerateTopicsName(int load)
        {
            string topicNumber = null;
            List<string> topicslist = new List<string>();
            for (int j = 1; j <= load; j++)
            {
                topicNumber = j.ToString().PadLeft(4, '0');
                topicslist.Add($"testtopic{topicNumber}");
            }
            return topicslist;
        }

        public static string stringGenerator(int size)
        {
            Random rand = new Random((int)DateTime.Now.Ticks);
            string input = "abcdefghijklmnopqrstuvwxyz0123456789";
            return new string(Enumerable.Range(0, size).Select(x => input[rand.Next(0, input.Length)]).ToArray());
        }
    }
}








