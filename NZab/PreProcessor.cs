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
using System.Threading.Tasks;

namespace NZab
{
    /**
     * This processor is used to let clients to convert requests into transaction
     * and hands the transaction to BroadcastProcessor to broadcast to all peers.
     */
    internal class PreProcessor : RequestProcessor
    {
        private readonly BlockingCollection<MessageTuple> requestQueue = new BlockingCollection<MessageTuple>();
        private readonly StateMachine stateMachine;
        private readonly IDictionary<string, PeerHandler> quorumMapOriginal;
        private readonly IDictionary<string, PeerHandler> quorumMap;
        private readonly Task task;
        private ClusterConfiguration clusterConfig;

        public PreProcessor(StateMachine stateMachine,
                            IDictionary<string, PeerHandler> quorumMap,
                            ClusterConfiguration config)
        {
            this.stateMachine = stateMachine;
            this.quorumMapOriginal = quorumMap;
            this.quorumMap = new Dictionary<string, PeerHandler>(quorumMap);
            this.clusterConfig = (ClusterConfiguration)config.Clone();
            this.task = Task.Factory.StartNew(Run, TaskCreationOptions.LongRunning);
        }

        public void ProcessRequest(MessageTuple request)
        {
            this.requestQueue.Add(request);
        }

        private void AddToQuorumSet(string serverId)
        {
            PeerHandler ph;
            if (this.quorumMapOriginal.TryGetValue(serverId, out ph))
            {
                this.quorumMap[serverId] = ph;
            }
        }

        private void RemoveFromQuorumSet(string serverId)
        {
            this.quorumMap.Remove(serverId);
        }

        private void Run()
        {
            try
            {
                while (true)
                {
                    MessageTuple request = requestQueue.Take();
                    if (request == MessageTuple.REQUEST_OF_DEATH) break;

                    Message msg = request.GetMessage();
                    string source = request.GetServerId();
                    Zxid zxid = request.GetZxid();

                    if (msg.GetType() == Message.MessageType.REQUEST)
                    {
                        var req = msg.GetRequest();
                        string clientId = request.GetServerId();
                        byte[] update = stateMachine.Preprocess(zxid, req.GetRequest());
                        Transaction txn = new Transaction(zxid, update);
                        Message prop = MessageBuilder.BuildProposal(txn, clientId);
                        foreach (PeerHandler ph in quorumMap.Values)
                        {
                            ph.QueueMessage(prop);
                        }
                    }
                    else if (msg.GetType() == Message.MessageType.JOIN)
                    {
                        if (!clusterConfig.Contains(source))
                        {
                            clusterConfig.AddPeer(source);
                        }
                        clusterConfig.SetVersion(zxid);
                        Message prop = MessageBuilder.BuildProposal(clusterConfig.ToTransaction());
                        foreach (PeerHandler ph in quorumMap.Values)
                        {
                            ph.QueueMessage(prop);
                        }
                        AddToQuorumSet(source);
                    }
                    else if (msg.GetType() == Message.MessageType.ACK_EPOCH)
                    {
                        AddToQuorumSet(source);
                    }
                    else if (msg.GetType() == Message.MessageType.REMOVE)
                    {
                        string peerId = msg.GetRemove().GetServerId();
                        string clientId = request.GetServerId();
                        clusterConfig.RemovePeer(peerId);
                        clusterConfig.SetVersion(zxid);
                        Message prop = MessageBuilder.BuildProposal(clusterConfig.ToTransaction(), clientId);
                        foreach (PeerHandler ph in quorumMap.Values)
                        {
                            ph.QueueMessage(prop);
                        }
                        RemoveFromQuorumSet(peerId);
                    }
                    else if (msg.GetType() == Message.MessageType.DISCONNECTED)
                    {
                        string peerId = msg.GetDisconnected().GetServerId();
                        RemoveFromQuorumSet(peerId);
                    }
                }
            }
            catch (Exception) { }
        }

        public void Shutdown()
        {
            this.requestQueue.Add(MessageTuple.REQUEST_OF_DEATH);
            this.task.Wait();
        }
    }
}
