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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using jzab;
using Microsoft.Extensions.Logging;

namespace NZab
{
    /**
     * Accepts acknowledgment from peers and broadcasts COMMIT message if there're
     * any committed transactions.
     */
    internal class AckProcessor : RequestProcessor
    {
        private static readonly ILogger LOG = LoggerFactory.Create(b =>
        {
            b.AddSimpleConsole();
            b.SetMinimumLevel(LogLevel.Debug);
        }).CreateLogger(typeof(AckProcessor));

        private readonly BlockingCollection<MessageTuple> ackQueue = new BlockingCollection<MessageTuple>();
        private readonly IDictionary<string, PeerHandler> quorumMapOriginal;
        private readonly IDictionary<string, PeerHandler> quorumMap;
        private ClusterConfiguration clusterConfig;
        private ClusterConfiguration pendingConfig;
        private readonly Task task;

        public AckProcessor(IDictionary<string, PeerHandler> quorumMap, ClusterConfiguration cnf)
        {
            this.quorumMapOriginal = quorumMap;
            this.quorumMap = new Dictionary<string, PeerHandler>(quorumMap);
            this.clusterConfig = (ClusterConfiguration)cnf.Clone();
            this.task = Task.Factory.StartNew(Run, TaskCreationOptions.LongRunning);
        }

        public void ProcessRequest(MessageTuple request)
        {
            this.ackQueue.Add(request);
        }

        private Zxid GetCommittedZxid(ClusterConfiguration cnf)
        {
            List<Zxid> zxids = new List<Zxid>();
            foreach (PeerHandler ph in quorumMap.Values)
            {
                Zxid ackZxid = ph.GetLastAckedZxid();
                if (cnf.Contains(ph.GetServerId()))
                {
                    if (ackZxid != null)
                    {
                        zxids.Add(ackZxid);
                    }
                }
            }
            int quorumSize = cnf.GetQuorumSize();
            if (quorumSize == 0)
            {
                return cnf.GetVersion();
            }
            if (zxids.Count < quorumSize)
            {
                return Zxid.ZXID_NOT_EXIST;
            }
            zxids.Sort();
            return zxids[zxids.Count - quorumSize];
        }

        private void Run()
        {
            try
            {
                while (true)
                {
                    MessageTuple request = ackQueue.Take();
                    if (request == MessageTuple.REQUEST_OF_DEATH) break;

                    Message msg = request.GetMessage();
                    string source = request.GetServerId();

                    if (msg.type == Message.MessageType.ACK)
                    {
                        var ack = msg.ack;
                        Zxid zxid = MessageBuilder.FromProtoZxid(ack.zxid);
                        this.quorumMap[source].SetLastAckedZxid(zxid);
                        Zxid zxidCanCommit = null;

                        if (this.pendingConfig != null)
                        {
                            zxidCanCommit = GetCommittedZxid(this.pendingConfig);
                            if (zxidCanCommit.CompareTo(pendingConfig.GetVersion()) >= 0)
                            {
                                this.clusterConfig = this.pendingConfig;
                                this.pendingConfig = null;
                            }
                            else
                            {
                                zxidCanCommit = null;
                            }
                        }

                        if (zxidCanCommit == null)
                        {
                            zxidCanCommit = GetCommittedZxid(this.clusterConfig);
                            if (pendingConfig != null && zxidCanCommit.CompareTo(pendingConfig.GetVersion()) >= 0)
                            {
                                Zxid version = pendingConfig.GetVersion();
                                if (version.GetXid() == 0) continue;
                                else zxidCanCommit = new Zxid(version.GetEpoch(), version.GetXid() - 1);
                            }
                        }

                        foreach (PeerHandler ph in quorumMap.Values)
                        {
                            if (ph.GetLastAckedZxid() == null) continue;
                            Zxid zxidCommit = zxidCanCommit;
                            if (zxidCommit.CompareTo(ph.GetLastAckedZxid()) > 0)
                            {
                                zxidCommit = ph.GetLastAckedZxid();
                            }
                            if (zxidCommit.CompareTo(ph.GetLastCommittedZxid()) > 0)
                            {
                                Message commit = MessageBuilder.BuildCommit(zxidCommit);
                                ph.QueueMessage(commit);
                                ph.SetLastCommittedZxid(zxidCommit);
                            }
                        }
                    }
                    else if (msg.type == Message.MessageType.JOIN || msg.type == Message.MessageType.ACK_EPOCH)
                    {
                        PeerHandler ph;
                        if (quorumMapOriginal.TryGetValue(source, out ph))
                        {
                            this.quorumMap[source] = ph;
                        }
                        if (msg.type == Message.MessageType.JOIN)
                        {
                            this.pendingConfig = (ClusterConfiguration)this.clusterConfig.Clone();
                            this.pendingConfig.AddPeer(source);
                            this.pendingConfig.SetVersion(request.GetZxid());
                        }
                    }
                    else if (msg.type == Message.MessageType.DISCONNECTED)
                    {
                        string peerId = msg.disconnected.server_id;
                        this.quorumMap.Remove(peerId);
                    }
                    else if (msg.type == Message.MessageType.REMOVE)
                    {
                        string srvId = msg.remove.serverId;
                        this.pendingConfig = (ClusterConfiguration)this.clusterConfig.Clone();
                        this.pendingConfig.RemovePeer(srvId);
                        this.pendingConfig.SetVersion(request.GetZxid());
                    }
                }
            }
            catch (Exception) { }
        }

        public void Shutdown()
        {
            this.ackQueue.Add(MessageTuple.REQUEST_OF_DEATH);
            this.task.Wait();
        }
    }
}
