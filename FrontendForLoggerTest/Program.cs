using Microsoft.AspNetCore.SignalR.Client;

var url = "http://localhost:5001/loggerhub"; // adjust if needed
var connection = new HubConnectionBuilder()
    .WithUrl(url)
    .WithAutomaticReconnect()
    .Build();

connection.On<string, string, string>("ReceiveLog", (level, source, message) =>
{
    Console.WriteLine($"[{level}] {source}: {message}");
});

await connection.StartAsync();
Console.WriteLine("Connected. Listening for logs. Press Ctrl+C to exit.");
await Task.Delay(-1);