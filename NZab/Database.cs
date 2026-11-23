#nullable enable

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json.Nodes;
using NZab;

using Microsoft.Extensions.Logging;

/**
 * StateMachine of the ZabKV.
 */
public sealed class Database : StateMachine
{
  private static readonly ILogger LOG = LoggerFactory.Create(b =>
  {
     b.AddSimpleConsole();
     b.SetMinimumLevel(LogLevel.Debug);
  }).CreateLogger(typeof(Database));

  private Zab zab;
  private string serverId;
  private readonly ZabConfig config = new ZabConfig();
  private readonly ConcurrentDictionary<string,Object> kvstore = new ();

  public Database(string serverId, string? joinPeer = null, string? logDir = null)
  {
    try
    {
      this.serverId = serverId;

      if (this.serverId != null && joinPeer == null)
      {
        // It's the first server in cluster, joins itself.
        joinPeer = this.serverId;
      }

      if (this.serverId != null && logDir == null)
      {
        // If user doesn't specify log directory, default one is
        // serverId in current directory.
        logDir = this.serverId;
      }

      config.SetLogDir(logDir!);

      if (joinPeer != null)
      {
        zab = new Zab(this, config, this.serverId, joinPeer);
      }
      else
      {
        // Recovers from log directory.
        zab = new Zab(this, config);
      }

      this.serverId = zab.GetServerId();
    }
    catch (Exception)
    {
      throw;
    }
  }

  // public string get(string key)
  // {
  //   GsonBuilder builder = new GsonBuilder();
  //   Gson gson = builder.create();
  //
  //   Map map = new HashMap();
  //   map.put(key, (object)kvstore.get(key));
  //
  //   return gson.toJson(map);
  // }
  //
  // public void put(Map updates)
  // {
  //   // updates: Map<String, byte[]>
  //   kvstore.putAll(updates);
  // }
  //
  // public string getAll()
  // {
  //   GsonBuilder builder = new GsonBuilder();
  //   Gson gson = builder.create();
  //   return gson.toJson(kvstore);
  // }
  //
  // public void delete(string key)
  // {
  //   if (key.Equals(""))
  //   {
  //     // Deletes whole database.
  //     kvstore.clear();
  //   }
  //   else
  //   {
  //     kvstore.remove(key);
  //   }
  // }

  public bool Add(byte[] bb, Object context)
  {
    try
    {
      zab.Send(bb, context);
    }
    catch (ZabException ze)
    {
      LOG.LogError(ze, "Exception in add");
      return false;
    }

    return true;
  }

  public byte[] Preprocess(Zxid zxid, byte[] message)
  {
    LOG.LogDebug("Preprocessing a message: {s}", message);
    return message;
  }

  public void Deliver(Zxid zxid, byte[] stateUpdate, string clientId, object ctx)
  {
    var ja = JsonNode.Parse(stateUpdate)?.AsArray();
    var key = ja?[0]?.GetValue<string>();
    var val = ja?[1]?.GetValue<Object>();

    if (key != null && val != null && kvstore.TryAdd(key, val))
    {
      LOG.LogDebug("Message delivered");
    }
    else
    {
      LOG.LogDebug("Deliver failed");
    }
  }

  public void Flushed(Zxid zxid, byte[] request, object ctx) {
  }

  public void Save(Stream os)
  {
    // No support for snapshot yet.
  }

  public void Restore(Stream @is)
  {
    // No support for snapshot yet.
  }

  public void SnapshotDone(string filePath, object ctx)
  {
  }

  public void Removed(string peerId, object ctx)
  {
  }

  public void Recovering(PendingRequests pendingRequests)
  {
    LOG.LogWarning("Recovering...");

    // Returns error for all pending requests.
    // for (java.util.Iterator it = pendingRequests.pendingSends.iterator(); it.hasNext();)
    // {
    //   PendingRequests.Tuple tp = (PendingRequests.Tuple)it.next();
    //   AsyncContext context = (AsyncContext)tp.param;
    //
    //   HttpServletResponse response = (HttpServletResponse)context.getResponse();
    //   response.setContentType("text/html");
    //   response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
    //   context.complete();
    // }
  }

  public void Leading(ISet<string> activeFollowers, ISet<string> clusterMembers)
  {
    LOG.LogInformation("LEADING with active followers {}", new object[] { activeFollowers.ToArray()});
    LOG.LogInformation("Cluster configuration change {}", new object[] { clusterMembers.ToArray() });
  }

  public void Following(string leader, ISet<string> clusterMembers)
  {
    LOG.LogInformation("FOLLOWING {s}", new object[] { leader });
    LOG.LogInformation("Cluster configuration change {}", new object[] { clusterMembers.ToArray() });
  }
}
