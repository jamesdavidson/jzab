using System;
using System.IO;
using System.Threading.Tasks;
using NZab;

// java.lang.System.setProperty("io.netty.noUnsafe", "true");

var d = new System.Collections.ArrayList();

var ports = new int[] { 5001, 5002, 5003 };

var peer = "";

var ip = (args.Length > 0) ? args[0] : "127.0.0.1";
for (int i = 0; i < ports.Length; i++)
{
    Console.Out.WriteLine(i);
    var dirName = $"logs-{ports[i]}";
    if (Directory.Exists(dirName))
    {
        Directory.Delete(dirName, true);
    }
    var logs1 = Directory.CreateDirectory(dirName).FullName;
    if (i == 0)
    {
        peer = $"{ip}:{ports[i]}";
        d.Add(new Database(peer, null, logs1));
    }
    else
    {
        d.Add(new Database($"{ip}:{ports[i]}", peer, logs1));
    }
}

var n = 10;
var bb = new byte[n];
for (int i = 0; i < n; i++)
{
    bb[i] = (byte)(i+65);
}

var db = (Database)d[0];

var bs = "[\"asdf\", 1234567890]"u8.ToArray();

bb = bs;

await Task.Delay(10*1000);

db.Add(bb, null);

await Task.Delay(5000);

Console.Out.WriteLine("Done");
