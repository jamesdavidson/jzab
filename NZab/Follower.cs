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
using jzab;

using Microsoft.Extensions.Logging;

namespace NZab
{
    /**
     * Follower.
     */
    internal class Follower : Participant
    {
        private static readonly ILogger LOG = LoggerFactory.Create(builder =>
        {
            builder.AddSimpleConsole();
            builder.SetMinimumLevel(LogLevel.Debug);
        }).CreateLogger(typeof(Follower));

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
            LOG.LogDebug("Follower joins in.");
            try
            {
                Message query = MessageBuilder.BuildQueryLeader();
                MessageTuple tuple = null;
                while (true)
                {
                    LOG.LogDebug("Query leader from {peer}", peer);
                    try
                    {
                        SendMessage(peer, query);
                        tuple = filter.GetExpectedMessage(Message.MessageType.QUERY_LEADER_REPLY, peer, config.GetTimeoutMs());
                        break;
                    }
                    catch (TimeoutException)
                    {
                        LOG.LogWarning("Timeout while contacting leader, going to retry after {retryInterval} second.", 1);
                        Thread.Sleep(1000);
                    }
                }
                this.electedLeader = tuple.GetMessage().reply.leader;
                LOG.LogDebug("Got current leader {leader}", this.electedLeader);

                while (true)
                {
                    try
                    {
                        JoinSynchronization();
                        break;
                    }
                    catch (Exception ex) when (ex is TimeoutException || ex is BackToElectionException)
                    {
                        LOG.LogDebug("Timeout({timeout} ms) in synchronizing, retrying. Last zxid: {zxid}",
                                     GetSyncTimeoutMs(),
                                     persistence.GetLatestZxid());
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
            catch (ThreadInterruptedException)
            {
                LOG.LogDebug("Participant is canceled by user.");
                throw;
            }
            catch (TimeoutException)
            {
                LOG.LogDebug("Didn't hear message from {leader} for {timeout} milliseconds. Going back to leader election.",
                             this.electedLeader,
                             this.config.GetTimeoutMs());
                if (persistence.GetLastSeenConfig() == null)
                {
                    throw new JoinFailure("Fails to join cluster.");
                }
            }
            catch (BackToElectionException)
            {
                LOG.LogDebug("Got GO_BACK message from queue, going back to electing.");
                if (persistence.GetLastSeenConfig() == null)
                {
                    throw new JoinFailure("Fails to join cluster.");
                }
            }
            catch (Participant.LeftCluster e)
            {
                LOG.LogDebug("Exit running: {message}", e.Message);
                throw;
            }
            catch (Exception e)
            {
                LOG.LogError(e, "Caught exception");
                throw;
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

                LOG.LogDebug("Synchronizing...");
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
            catch (ThreadInterruptedException)
            {
                LOG.LogDebug("Participant is canceled by user.");
                throw;
            }
            catch (TimeoutException)
            {
                LOG.LogDebug("Didn't hear message from {leader} for {timeout} milliseconds. Going back to leader election.",
                             this.electedLeader,
                             this.config.GetTimeoutMs());
            }
            catch (BackToElectionException)
            {
                LOG.LogDebug("Got GO_BACK message from queue, going back to electing.");
            }
            catch (Zab.SimulatedException)
            {
                LOG.LogDebug("Got SimulatedException, go back to leader election.");
            }
            catch (Participant.LeftCluster e)
            {
                LOG.LogDebug("Exit running: {message}", e.Message);
                throw;
            }
            catch (Exception e)
            {
                LOG.LogError(e, "Caught exception");
                throw;
            }
            finally
            {
                if (this.currentPhase == Phase.SYNCHRONIZING)
                {
                    IncSyncTimeout();
                    LOG.LogDebug("Go back to recovery in synchronization phase, increase sync timeout to {timeout} milliseconds.",
                                 GetSyncTimeoutMs());
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
            LOG.LogDebug("Sends {message} to leader {leader}", message, this.electedLeader);
            SendMessage(this.electedLeader, message);
        }

        void WaitForNewEpoch()
        {
            MessageTuple tuple = filter.GetExpectedMessage(Message.MessageType.NEW_EPOCH, this.electedLeader, config.GetTimeoutMs());
            var epoch = tuple.GetMessage().new_epoch;
            if (epoch.new_epoch < persistence.GetProposedEpoch())
            {
                LOG.LogError("New epoch {newEpoch} from {source} is smaller than last received proposed epoch {proposedEpoch}",
                             epoch.new_epoch,
                             tuple.GetServerId(),
                             persistence.GetProposedEpoch());
                throw new Exception("New epoch is smaller than current one.");
            }
            persistence.SetProposedEpoch(epoch.new_epoch);
            SetSyncTimeoutMs(epoch.sync_timeout);
            LOG.LogDebug("Received the new epoch proposal {epoch} from {source}.",
                         epoch.new_epoch,
                         tuple.GetServerId());
            Zxid zxid = persistence.GetLatestZxid();
            SendMessage(this.electedLeader, MessageBuilder.BuildAckEpoch(persistence.GetAckEpoch(), zxid));
        }

        void WaitForNewLeaderMessage()
        {
            MessageTuple tuple = filter.GetExpectedMessage(Message.MessageType.NEW_LEADER, this.electedLeader, config.GetTimeoutMs());
            LOG.LogDebug("Waiting for New Leader message from {leader}.", this.electedLeader);
            LOG.LogDebug("Got message {message} from {source}.",
                         tuple.GetMessage(),
                         tuple.GetServerId());
            long epoch = tuple.GetMessage().new_leader.epoch;
            persistence.GetLog().Sync();
            persistence.SetAckEpoch(epoch);
            Message ack = MessageBuilder.BuildAck(persistence.GetLatestZxid());
            SendMessage(tuple.GetServerId(), ack);
        }

        void WaitForCommitMessage()
        {
            MessageTuple tuple = filter.GetExpectedMessage(Message.MessageType.COMMIT, this.electedLeader, config.GetTimeoutMs());
            LOG.LogDebug("Waiting for commit message from {leader}", this.electedLeader);
            Zxid zxid = MessageBuilder.FromProtoZxid(tuple.GetMessage().commit.zxid);
            Zxid lastZxid = persistence.GetLatestZxid();
            if (zxid.CompareTo(lastZxid) != 0)
            {
                LOG.LogError("The ACK zxid {zxid} doesn't match last zxid {lastZxid} in log!",
                             zxid,
                             lastZxid);
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
                            LOG.LogWarning("Detects there's a timeout in waiting message from leader {leader}, goes back to leader electing",
                                           this.electedLeader);
                            throw new TimeoutException("HEARTBEAT timeout!");
                        }
                        if (msg.type == Message.MessageType.QUERY_LEADER)
                        {
                            LOG.LogDebug("Got QUERY_LEADER from {source}", source);
                            Message reply = MessageBuilder.BuildQueryReply(this.electedLeader);
                            SendMessage(source, reply);
                        }
                        else if (msg.type == Message.MessageType.ELECTION_INFO)
                        {
                            this.election.Reply(tuple);
                        }
                        else if (msg.type == Message.MessageType.DELIVERED)
                        {
                            OnDelivered(msg);
                        }
                        else if (msg.type == Message.MessageType.SNAPSHOT)
                        {
                            snapProcessor.ProcessRequest(tuple);
                        }
                        else if (msg.type == Message.MessageType.SNAPSHOT_DONE)
                        {
                            commitProcessor.ProcessRequest(tuple);
                        }
                        else
                        {
                            LOG.LogDebug("Got unexpected message from {source}, ignores.", source);
                        }
                        continue;
                    }

                    if (msg.type != Message.MessageType.HEARTBEAT)
                    {
                        LOG.LogDebug("Got message {message} from {source}", msg, source);
                    }
                    if (msg.type == Message.MessageType.PROPOSAL)
                    {
                        Transaction txn = MessageBuilder.FromProposal(msg.proposal);
                        if (txn.GetZxid().GetEpoch() == ackEpoch)
                        {
                            OnProposal(tuple);
                        }
                        else
                        {
                            LOG.LogDebug("The proposal has the wrong epoch {epoch}, expecting {ackEpoch}.",
                                         txn.GetZxid().GetEpoch(),
                                         ackEpoch);
                            throw new Exception("The proposal has wrong epoch number.");
                        }
                    }
                    else if (msg.type == Message.MessageType.COMMIT)
                    {
                        commitProcessor.ProcessRequest(tuple);
                    }
                    else if (msg.type == Message.MessageType.HEARTBEAT)
                    {
                        LOG.LogTrace("Got HEARTBEAT from {source}.", source);
                        Message heartbeatReply = MessageBuilder.BuildHeartbeat();
                        SendMessage(source, heartbeatReply);
                    }
                    else if (msg.type == Message.MessageType.FLUSH)
                    {
                        OnFlush(tuple);
                    }
                    else
                    {
                        LOG.LogWarning("Unexpected message: {message} from {source}", msg, source);
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
            SetSyncTimeoutMs(tuple.GetMessage().sync_history_reply.sync_timeout);
            WaitForSync(this.electedLeader);

            Zxid lastZxid = persistence.GetLatestZxid();
            LOG.LogDebug("After first synchronization, the last zxid is {zxid}", lastZxid);
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
                    if (tuple.GetMessage().type == Message.MessageType.DISCONNECTED)
                    {
                        string peerId = tuple.GetMessage().disconnected.server_id;
                        if (follower.electedLeader != null && peerId.Equals(follower.electedLeader))
                        {
                            LOG.LogDebug("Lost elected leader {leader}.", follower.electedLeader);
                            throw new BackToElectionException();
                        }
                        else
                        {
                            LOG.LogDebug("Lost peer {peer}.", peerId);
                            follower.transport.Clear(peerId);
                        }
                    }
                    else if (tuple.GetMessage().type == Message.MessageType.PROPOSED_EPOCH)
                    {
                        LOG.LogDebug("Got PROPOSED_EPOCH in FOLLOWING state. Close connection.");
                        follower.transport.Clear(tuple.GetServerId());
                    }
                    else return tuple;
                }
            }
        }
    }
}
