/**
 * Licensed to the zk1931 under one or more contributor license
 * agreements. See the NOTICE file distributed with this work
 * for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.  You may obtain a copy of the
 * License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Concurrent;

namespace NZab
{
    /**
     * This class is used to deliver committed transaction.
     */
    internal class CommitProcessor : RequestProcessor
    {
        private readonly BlockingCollection<MessageTuple> commitQueue = new BlockingCollection<MessageTuple>();
        private readonly StateMachine stateMachine;
        private Zxid lastDeliveredZxid = Zxid.ZXID_NOT_EXIST;
        private readonly List<ZabMessage.Proposal> pendingTxns = new List<ZabMessage.Proposal>();
        private readonly string serverId;
        private readonly Transport transport;
        private readonly HashSet<string> quorumSet;
        private ClusterConfiguration clusterConfig;
        private readonly string leader;
        private readonly Queue<PendingFlush> flushQueue = new Queue<PendingFlush>();
        private readonly PendingRequests pendings;
        private readonly Task task;

        public CommitProcessor(StateMachine stateMachine,
                               Zxid lastDeliveredZxid,
                               string serverId,
                               Transport transport,
                               HashSet<string> quorumSet,
                               ClusterConfiguration clusterConfig,
                               string leader,
                               PendingRequests pendings)
        {
            this.stateMachine = stateMachine;
            this.lastDeliveredZxid = lastDeliveredZxid;
            this.serverId = serverId;
            this.transport = transport;
            this.clusterConfig = (ClusterConfiguration)clusterConfig.Clone();
            this.leader = leader;
            this.pendings = pendings;
            this.quorumSet = quorumSet != null ? new HashSet<string>(quorumSet) : null;
            this.task = Task.Factory.StartNew(Run, TaskCreationOptions.LongRunning);
        }

        public void ProcessRequest(MessageTuple request)
        {
            this.commitQueue.Add(request);
        }

        private void Run()
        {
            try
            {
                while (true)
                {
                    MessageTuple request = this.commitQueue.Take();
                    if (request == MessageTuple.REQUEST_OF_DEATH) break;

                    Message msg = request.GetMessage();
                    string source = request.GetServerId();

                    if (msg.GetType() == Message.MessageType.PROPOSAL)
                    {
                        this.pendingTxns.Add(msg.GetProposal());
                    }
                    else if (msg.GetType() == Message.MessageType.COMMIT)
                    {
                        var commit = msg.GetCommit();
                        Zxid zxid = MessageBuilder.FromProtoZxid(commit.GetZxid());
                        if (zxid.CompareTo(this.lastDeliveredZxid) <= 0) continue;

                        int i = 0;
                        while (i < this.pendingTxns.Count)
                        {
                            var prop = this.pendingTxns[i];
                            Transaction txn = MessageBuilder.FromProposal(prop);
                            if (zxid.CompareTo(txn.GetZxid()) < 0) break;

                            if (txn.GetTransactionType() == 0 /* USER_REQUEST_VALUE */)
                            {
                                DeliverTxn(txn, prop.GetClientId());
                            }
                            else if (txn.GetTransactionType() == 1 /* COP_VALUE */)
                            {
                                DeliverConfiguration(txn, prop.GetClientId());
                                if (!clusterConfig.Contains(this.serverId))
                                {
                                    transport.Send(this.serverId, MessageBuilder.BuildShutDown());
                                    return;
                                }
                            }
                            this.lastDeliveredZxid = txn.GetZxid();
                            DeliverPendingFlushes(lastDeliveredZxid);
                            i++;
                        }
                        this.pendingTxns.RemoveRange(0, i);
                        transport.Send(this.serverId, MessageBuilder.BuildDelivered(lastDeliveredZxid));
                    }
                    else if (msg.GetType() == Message.MessageType.ACK_EPOCH)
                    {
                        quorumSet.Add(source);
                        NotifyClient();
                    }
                    else if (msg.GetType() == Message.MessageType.DISCONNECTED)
                    {
                        string peerId = msg.GetDisconnected().GetServerId();
                        quorumSet.Remove(peerId);
                        NotifyClient();
                    }
                    else if (msg.GetType() == Message.MessageType.JOIN)
                    {
                        quorumSet.Add(source);
                    }
                    else if (msg.GetType() == Message.MessageType.FLUSH)
                    {
                        var flush = msg.GetFlush();
                        Zxid zxid = MessageBuilder.FromProtoZxid(flush.GetZxid());
                        flushQueue.Enqueue(new PendingFlush(zxid, flush.GetBody()));
                        DeliverPendingFlushes(lastDeliveredZxid);
                    }
                    else if (msg.GetType() == Message.MessageType.SNAPSHOT_DONE)
                    {
                        object ctx = pendings.PendingSnapshots[0];
                        pendings.PendingSnapshots.RemoveAt(0);
                        stateMachine.SnapshotDone(msg.GetSnapshotDone().GetFilePath(), ctx);
                    }
                }
            }
            catch (Exception) { }
        }

        void NotifyClient()
        {
            if (quorumSet == null)
            {
                stateMachine.Following(this.leader, new HashSet<string>(clusterConfig.GetPeers()));
            }
            else
            {
                stateMachine.Leading(new HashSet<string>(quorumSet), new HashSet<string>(clusterConfig.GetPeers()));
            }
        }

        public void Shutdown()
        {
            this.commitQueue.Add(MessageTuple.REQUEST_OF_DEATH);
            this.task.Wait();
        }

        void DeliverTxn(Transaction txn, string clientId)
        {
            object ctx = null;
            if (clientId != null && clientId.Equals(serverId))
            {
                var tp = pendings.PendingSends[0];
                pendings.PendingSends.RemoveAt(0);
                ctx = tp.Ctx;
            }
            stateMachine.Deliver(txn.GetZxid(), txn.GetBody(), clientId, ctx);
        }

        void DeliverConfiguration(Transaction txn, string clientId)
        {
            ClusterConfiguration cnf = ClusterConfiguration.FromByteArray(txn.GetBody(), "");
            if (clientId != null && clientId.Equals(this.serverId))
            {
                var tp = pendings.PendingRemoves[0];
                pendings.PendingRemoves.RemoveAt(0);
                stateMachine.Removed((string)tp.Param, tp.Ctx);
            }
            clusterConfig = cnf;
            NotifyClient();
        }

        void DeliverPendingFlushes(Zxid zxid)
        {
            while (flushQueue.Count > 0 && flushQueue.Peek().WaitZxid.CompareTo(zxid) <= 0)
            {
                PendingFlush flush = flushQueue.Dequeue();
                var tp = pendings.PendingFlushes[0];
                pendings.PendingFlushes.RemoveAt(0);
                stateMachine.Flushed(flush.WaitZxid, flush.Body, tp.Ctx);
            }
        }

        public Zxid GetLastDeliveredZxid() { return this.lastDeliveredZxid; }

        class PendingFlush
        {
            public readonly Zxid WaitZxid;
            public readonly byte[] Body;
            public PendingFlush(Zxid waitZxid, byte[] body)
            {
                this.WaitZxid = waitZxid;
                this.Body = body;
            }
        }
    }
}
