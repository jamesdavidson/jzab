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
using System.Threading.Tasks;
using jzab;

namespace NZab
{
    /**
     * This class manages the logic synchronizing the transactions to disk. It can
     * batch several transactions and sync to disk once.
     */
    internal class SyncProposalProcessor : RequestProcessor
    {
        private readonly ILog log;
        private readonly PersistentState persistence;
        private readonly BlockingCollection<MessageTuple> proposalQueue = new BlockingCollection<MessageTuple>();
        private readonly Task task;
        private readonly Transport transport;
        private readonly int maxBatchSize;
        private readonly string serverId;

        public SyncProposalProcessor(PersistentState persistence, Transport transport, int maxBatchSize)
        {
            this.persistence = persistence;
            this.log = persistence.GetLog();
            this.transport = transport;
            this.maxBatchSize = maxBatchSize;
            this.serverId = persistence.GetLastSeenConfig().GetServerId();
            this.task = Task.Factory.StartNew(Run, TaskCreationOptions.LongRunning);
        }

        public void ProcessRequest(MessageTuple request)
        {
            proposalQueue.Add(request);
        }

        void SendAck(string source, Zxid ackZxid)
        {
            Message ack = MessageBuilder.BuildAck(ackZxid);
            this.transport.Send(source, ack);
        }

        private void Run()
        {
            try
            {
                MessageTuple lastReq = null;
                int batchCount = 0;

                while (true)
                {
                    MessageTuple req;
                    if (lastReq == null)
                    {
                        req = proposalQueue.Take();
                    }
                    else
                    {
                        if (!proposalQueue.TryTake(out req, 0))
                        {
                            if (batchCount > 0)
                            {
                                this.log.Sync();
                                Zxid zxid = MessageBuilder.FromProtoZxid(lastReq.GetMessage().proposal.zxid);
                                SendAck(lastReq.GetServerId(), zxid);
                                batchCount = 0;
                            }
                            req = proposalQueue.Take();
                        }
                        else if (batchCount == maxBatchSize || req == MessageTuple.REQUEST_OF_DEATH)
                        {
                            this.log.Sync();
                            Zxid zxid = MessageBuilder.FromProtoZxid(lastReq.GetMessage().proposal.zxid);
                            SendAck(lastReq.GetServerId(), zxid);
                            batchCount = 0;
                        }
                    }

                    if (req == MessageTuple.REQUEST_OF_DEATH) break;

                    if (req.GetMessage().type == Message.MessageType.PROPOSAL)
                    {
                        var prop = req.GetMessage().proposal;
                        Transaction txn = MessageBuilder.FromProposal(prop);
                        batchCount++;
                        lastReq = req;

                        if (txn.GetTransactionType() == 1 /* COP_VALUE */)
                        {
                            ClusterConfiguration cnf = ClusterConfiguration.FromByteArray(txn.GetBody(), this.serverId);
                            persistence.SetLastSeenConfig(cnf);
                            SendAck(req.GetServerId(), txn.GetZxid());
                            batchCount = 0;
                            lastReq = null;
                        }
                        this.log.Append(txn);
                    }
                }
            }
            catch (Exception) { }
        }

        public void Shutdown()
        {
            proposalQueue.Add(MessageTuple.REQUEST_OF_DEATH);
            task.Wait();
        }
    }
}
