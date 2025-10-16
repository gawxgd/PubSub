using Microsoft.AspNetCore.SignalR.Client;

var url = "http://localhost:5022/hubs/logs"; // adjust if needed
var connection = new HubConnectionBuilder()
    .WithUrl(url)
    .WithAutomaticReconnect()
    .Build();

connection.On<object>("log", evt => Console.WriteLine($"LOG: {System.Text.Json.JsonSerializer.Serialize(evt)}"));

await connection.StartAsync();
Console.WriteLine("Connected. Listening for logs. Press Ctrl+C to exit.");
await Task.Delay(-1);