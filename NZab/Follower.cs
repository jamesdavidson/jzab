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
using System.IO;
using System.Threading;

namespace NZab
{
    /**
     * Follower.
     */
    internal class Follower : Participant
    {
        public Follower(ParticipantState participantState,
                        StateMachine stateMachine,
                        ZabConfig config)
            : base(participantState, stateMachine, config)
        {
            filter = new FollowerFilter(messageQueue, election, this);
        }

        protected override void ChangePhase(Phase phase)
        {
            this.currentPhase = phase;
            if (phase == Phase.DISCOVERING)
            {
                if (stateChangeCallback != null)
                {
                    stateChangeCallback.FollowerDiscovering(this.electedLeader);
                }
                if (failCallback != null)
                {
                    failCallback.FollowerDiscovering();
                }
            }
            else if (phase == Phase.SYNCHRONIZING)
            {
                if (stateChangeCallback != null)
                {
                    stateChangeCallback.FollowerSynchronizing(persistence.GetProposedEpoch());
                }
                if (failCallback != null)
                {
                    failCallback.FollowerSynchronizing();
                }
            }
            else if (phase == Phase.BROADCASTING)
            {
                if (stateChangeCallback != null)
                {
                    stateChangeCallback.FollowerBroadcasting(persistence.GetAckEpoch(),
                                                             GetAllTxns(),
                                                             persistence.GetLastSeenConfig());
                }
                if (failCallback != null)
                {
                    failCallback.FollowerBroadcasting();
                }
            }
            else if (phase == Phase.FINALIZING)
            {
                stateMachine.Recovering(pendings);
                if (persistence.IsInStateTransfer())
                {
                    persistence.UndoStateTransfer();
                }
                if (this.electedLeader != null)
                {
                    this.transport.Clear(this.electedLeader);
                }
            }
        }

        public override void Join(string peer)
        {
            try
            {
                Message query = MessageBuilder.BuildQueryLeader();
                MessageTuple tuple = null;
                while (true)
                {
                    try
                    {
                        SendMessage(peer, query);
                        tuple = filter.GetExpectedMessage(Message.MessageType.QUERY_LEADER_REPLY, peer, config.GetTimeoutMs());
                        break;
                    }
                    catch (TimeoutException)
                    {
                        Thread.Sleep(1000);
                    }
                }
                this.electedLeader = tuple.GetMessage().GetReply().GetLeader();

                while (true)
                {
                    try
                    {
                        JoinSynchronization();
                        break;
                    }
                    catch (Exception ex) when (ex is TimeoutException || ex is BackToElectionException)
                    {
                        transport.Clear(electedLeader);
                        ClearMessageQueue();
                    }
                }
                RestoreFromSnapshot();
                DeliverUndeliveredTxns();

                this.election.SpecifyLeader(this.electedLeader);
                ChangePhase(Phase.BROADCASTING);
                Accepting();
            }
            finally
            {
                ChangePhase(Phase.FINALIZING);
            }
        }

        public void Follow(string leader)
        {
            this.electedLeader = leader;
            try
            {
                ChangePhase(Phase.DISCOVERING);
                SendProposedEpoch();
                WaitForNewEpoch();

                ChangePhase(Phase.SYNCHRONIZING);
                long st = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
                WaitForSync(this.electedLeader);
                WaitForNewLeaderMessage();
                WaitForCommitMessage();
                long syncTime = (DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond) - st;
                AdjustSyncTimeout((int)syncTime);

                RestoreFromSnapshot();
                DeliverUndeliveredTxns();

                ChangePhase(Phase.BROADCASTING);
                Accepting();
            }
            finally
            {
                if (this.currentPhase == Phase.SYNCHRONIZING)
                {
                    IncSyncTimeout();
                }
                ChangePhase(Phase.FINALIZING);
            }
        }

        void SendProposedEpoch()
        {
            Message message = MessageBuilder.BuildProposedEpoch(persistence.GetProposedEpoch(),
                                                                persistence.GetAckEpoch(),
                                                                persistence.GetLastSeenConfig(),
                                                                GetSyncTimeoutMs());
            SendMessage(this.electedLeader, message);
        }

        void WaitForNewEpoch()
        {
            MessageTuple tuple = filter.GetExpectedMessage(Message.MessageType.NEW_EPOCH, this.electedLeader, config.GetTimeoutMs());
            var epoch = tuple.GetMessage().GetNewEpoch();
            if (epoch.GetNewEpoch() < persistence.GetProposedEpoch())
            {
                throw new Exception("New epoch is smaller than current one.");
            }
            persistence.SetProposedEpoch(epoch.GetNewEpoch());
            SetSyncTimeoutMs(epoch.GetSyncTimeout());
            Zxid zxid = persistence.GetLatestZxid();
            SendMessage(this.electedLeader, MessageBuilder.BuildAckEpoch(persistence.GetAckEpoch(), zxid));
        }

        void WaitForNewLeaderMessage()
        {
            MessageTuple tuple = filter.GetExpectedMessage(Message.MessageType.NEW_LEADER, this.electedLeader, config.GetTimeoutMs());
            long epoch = tuple.GetMessage().GetNewLeader().GetEpoch();
            persistence.GetLog().Sync();
            persistence.SetAckEpoch(epoch);
            Message ack = MessageBuilder.BuildAck(persistence.GetLatestZxid());
            SendMessage(this.electedLeader, ack);
        }

        void WaitForCommitMessage()
        {
            MessageTuple tuple = filter.GetExpectedMessage(Message.MessageType.COMMIT, this.electedLeader, config.GetTimeoutMs());
            Zxid zxid = MessageBuilder.FromProtoZxid(tuple.GetMessage().GetCommit().GetZxid());
            Zxid lastZxid = persistence.GetLatestZxid();
            if (zxid.CompareTo(lastZxid) != 0)
            {
                throw new Exception("The ACK zxid doesn't match last zxid");
            }
        }

        void AcceptingInit()
        {
            this.clusterConfig = persistence.GetLastSeenConfig();
            this.syncProcessor = new SyncProposalProcessor(persistence, transport, maxBatchSize);
            this.commitProcessor = new CommitProcessor(stateMachine, lastDeliveredZxid, serverId, transport, null, clusterConfig, electedLeader, pendings);
            this.snapProcessor = new SnapshotProcessor(stateMachine, persistence, serverId, transport);
            stateMachine.Following(electedLeader, new HashSet<string>(clusterConfig.GetPeers()));
        }

        void Accepting()
        {
            AcceptingInit();
            long lastHeartbeatTime = DateTime.Now.Ticks * 100;
            long ackEpoch = persistence.GetAckEpoch();
            try
            {
                while (true)
                {
                    MessageTuple tuple = filter.GetMessage(config.GetTimeoutMs());
                    Message msg = tuple.GetMessage();
                    string source = tuple.GetServerId();
                    if (source.Equals(this.electedLeader))
                    {
                        lastHeartbeatTime = DateTime.Now.Ticks * 100;
                    }
                    else
                    {
                        long timeDiff = (DateTime.Now.Ticks * 100 - lastHeartbeatTime) / 1000000L;
                        if ((int)timeDiff >= this.config.GetTimeoutMs())
                        {
                            throw new TimeoutException("HEARTBEAT timeout!");
                        }
                        if (msg.GetType() == Message.MessageType.QUERY_LEADER)
                        {
                            Message reply = MessageBuilder.BuildQueryReply(this.electedLeader);
                            SendMessage(source, reply);
                        }
                        else if (msg.GetType() == Message.MessageType.ELECTION_INFO)
                        {
                            this.election.Reply(tuple);
                        }
                        else if (msg.GetType() == Message.MessageType.DELIVERED)
                        {
                            OnDelivered(msg);
                        }
                        else if (msg.GetType() == Message.MessageType.SNAPSHOT)
                        {
                            snapProcessor.ProcessRequest(tuple);
                        }
                        else if (msg.GetType() == Message.MessageType.SNAPSHOT_DONE)
                        {
                            commitProcessor.ProcessRequest(tuple);
                        }
                        continue;
                    }

                    if (msg.GetType() == Message.MessageType.PROPOSAL)
                    {
                        Transaction txn = MessageBuilder.FromProposal(msg.GetProposal());
                        if (txn.GetZxid().GetEpoch() == ackEpoch)
                        {
                            OnProposal(tuple);
                        }
                        else
                        {
                            throw new Exception("The proposal has wrong epoch number.");
                        }
                    }
                    else if (msg.GetType() == Message.MessageType.COMMIT)
                    {
                        commitProcessor.ProcessRequest(tuple);
                    }
                    else if (msg.GetType() == Message.MessageType.HEARTBEAT)
                    {
                        Message heartbeatReply = MessageBuilder.BuildHeartbeat();
                        SendMessage(source, heartbeatReply);
                    }
                    else if (msg.GetType() == Message.MessageType.FLUSH)
                    {
                        OnFlush(tuple);
                    }
                }
            }
            finally
            {
                commitProcessor.Shutdown();
                syncProcessor.Shutdown();
                snapProcessor.Shutdown();
                this.lastDeliveredZxid = commitProcessor.GetLastDeliveredZxid();
                this.participantState.UpdateLastDeliveredZxid(this.lastDeliveredZxid);
            }
        }

        private void JoinSynchronization()
        {
            Message sync = MessageBuilder.BuildSyncHistory(persistence.GetLatestZxid());
            SendMessage(this.electedLeader, sync);
            MessageTuple tuple = filter.GetExpectedMessage(Message.MessageType.SYNC_HISTORY_REPLY, electedLeader, config.GetTimeoutMs());
            setSyncTimeoutMs(tuple.GetMessage().GetSyncHistoryReply().GetSyncTimeout());
            WaitForSync(this.electedLeader);

            Zxid lastZxid = persistence.GetLatestZxid();
            Message join = MessageBuilder.BuildJoin(lastZxid);
            SendMessage(this.electedLeader, join);

            ChangePhase(Phase.SYNCHRONIZING);
            WaitForSync(this.electedLeader);
            WaitForNewLeaderMessage();
            WaitForCommitMessage();
            persistence.SetProposedEpoch(persistence.GetAckEpoch());
        }

        class FollowerFilter : ElectionMessageFilter
        {
            private readonly Follower follower;
            public FollowerFilter(BlockingCollection<MessageTuple> msgQueue, Election election, Follower follower)
                : base(msgQueue, election)
            {
                this.follower = follower;
            }

            public override MessageTuple GetMessage(int timeoutMs)
            {
                long startMs = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
                while (true)
                {
                    long nowMs = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
                    int remainMs = timeoutMs - (int)(nowMs - startMs);
                    if (remainMs < 0) remainMs = 0;
                    MessageTuple tuple = base.GetMessage(remainMs);
                    if (tuple.GetMessage().GetType() == Message.MessageType.DISCONNECTED)
                    {
                        string peerId = tuple.GetMessage().GetDisconnected().GetServerId();
                        if (follower.electedLeader != null && peerId.Equals(follower.electedLeader))
                        {
                            throw new BackToElectionException();
                        }
                        else
                        {
                            follower.transport.Clear(peerId);
                        }
                    }
                    else if (tuple.GetMessage().GetType() == Message.MessageType.PROPOSED_EPOCH)
                    {
                        follower.transport.Clear(tuple.GetServerId());
                    }
                    else return tuple;
                }
            }
        }
    }
}
