using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Buffers.Binary;
using System.Threading.Tasks;
using System.Text.Json;
using System.Net;
using System.Collections.Concurrent;

namespace AbxExchangeClient
{
    public class AbxPacket
    {
        public string Symbol { get; set; }
        public char Side { get; set; }
        public int Quantity { get; set; }
        public int Price { get; set; }
        public int Sequence { get; set; }
    }

    public class AbxClient
    {

        
        private static string Host = Dns.GetHostName();//Taking HostName of the system        
        private static string OutputFile = AppDomain.CurrentDomain.BaseDirectory + "abx_output.json";//setting file path
        private static int Port = 3000;// setting the port 

        static AbxClient()
        {
            /* code to fetch IP address if connection did not established through hostname 
            IPAddress[] addresses = Dns.GetHostAddresses(Host);

            foreach (IPAddress ip in addresses)
            {
                if (ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
                {
                    Host = ip.ToString();
                }
            }*/
        }
        public async Task RunAsync()
        {
            List<AbxPacket> allPackets = new();

            try
            {
                Console.WriteLine("Connecting to ABX server to stream all packets...");
                using (var client = new TcpClient())
                {
                    await client.ConnectAsync(Host, Port);//Connecting to abx_exchange_server
                    using var stream = client.GetStream();

                    await SendStreamAllPacketsRequestAsync(stream);
                    allPackets = await ReceivePacketsAsync(stream);
                }// automatically close the connection

                Console.WriteLine($"Received {allPackets.Count} packets.");

                var missingSequences = GetMissingSequences(allPackets);
                if (missingSequences.Count > 0)//Check if packets are missing
                {
                    Console.WriteLine($"Missing sequences: {string.Join(", ", missingSequences)}");

                    foreach (var seq in missingSequences)
                    {
                        var packet = await RequestMissingPacketAsync(seq);
                        if (packet != null)
                        {
                            allPackets.Add(packet);
                            Console.WriteLine($"Received missing packet: Seq {packet.Sequence}");
                        }
                        else
                        {
                            Console.WriteLine($"Failed to receive missing packet: Seq {seq}");
                        }
                    }
                }
                var finalPackets = allPackets.OrderBy(p => p.Sequence).ToList();
                Console.WriteLine("Final ordered packet list:");
                foreach (var packet in finalPackets)
                {
                    Console.WriteLine($"Seq: {packet.Sequence}, Symbol: {packet.Symbol}, Side: {packet.Side}, Qty: {packet.Quantity}, Price: {packet.Price}");
                }

                await SaveToJsonAsync(finalPackets);
                Console.WriteLine($"Output written to {OutputFile}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] {ex.Message} \n [STACK TRACE] {ex.StackTrace}");
            }
        }

        private async Task SendStreamAllPacketsRequestAsync(NetworkStream stream)
        {
            try
            {
                byte[] request = new byte[2] { 1, 0 }; // callType = 1, resendSeq = 0 Request to stream all available packets from the server
                await stream.WriteAsync(request);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] Failed to send stream all request: {ex.Message}");
            }
        }

        private async Task<List<AbxPacket>> ReceivePacketsAsync(NetworkStream stream)
        {
            List<AbxPacket> packets = new();
            byte[] buffer = new byte[17];
            int bytesRead;

            try
            {
                while ((bytesRead = await stream.ReadAsync(buffer)) == 17)
                {
                    var packet = ParsePacket(buffer);
                    if (packet != null)
                        packets.Add(packet);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] Failed during packet reception: {ex.Message}");
            }

            return packets;
        }

        private AbxPacket ParsePacket(byte[] data)
        {
            try
            {
                string symbol = Encoding.ASCII.GetString(data, 0, 4);
                char side = (char)data[4];
                int quantity = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(5, 4));
                int price = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(9, 4));
                int sequence = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(13, 4));

                return new AbxPacket
                {
                    Symbol = symbol,
                    Side = side,
                    Quantity = quantity,
                    Price = price,
                    Sequence = sequence
                };
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] Failed to parse packet: {ex.Message}");
                return null;
            }
        }

        private List<int> GetMissingSequences(List<AbxPacket> packets)
        {
            var sequences = packets.Select(p => p.Sequence).OrderBy(n => n).ToList();
            var missing = new List<int>();

            for (int i = sequences.First(); i < sequences.Last(); i++)
            {
                if (!sequences.Contains(i))
                    missing.Add(i);
            }

            return missing;
        }

        private async Task<AbxPacket?> RequestMissingPacketAsync(int sequence)
        {
            try
            {
                using var client = new TcpClient();
                await client.ConnectAsync(Host, Port);
                using var stream = client.GetStream();

                byte[] request = new byte[2] { 2, (byte)sequence }; // callType = 2 Request to resend a specific packet with a given sequence number
                await stream.WriteAsync(request);

                byte[] buffer = new byte[17];
                int bytesRead = await stream.ReadAsync(buffer);

                return bytesRead == 17 ? ParsePacket(buffer) : null;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] Failed to request missing packet for sequence {sequence}: {ex.Message}");
                return null;
            }
        }

        private async Task SaveToJsonAsync(List<AbxPacket> packets)
        {
            try
            {
                var json = JsonSerializer.Serialize(packets, new JsonSerializerOptions { WriteIndented = true });
                await File.WriteAllTextAsync(OutputFile, json); // storing the file at OutputFile path
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] Failed to write JSON output: {ex.Message}");
            }
        }
    }

    
}
