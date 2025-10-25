using Microsoft.AspNetCore.SignalR.Client;

var url = "http://localhost:5000/loggerhub";
var connection = new HubConnectionBuilder()
    .WithUrl(url)
    .WithAutomaticReconnect()
    .Build();

connection.On<string, string, string>("ReceiveLog", (level, source, message) =>
{
    Console.WriteLine($"[{level}] {source}: {message}");
});

await connection.StartAsync();
Console.WriteLine("Connected to SignalR hub at " + url);
await Task.Delay(-1);