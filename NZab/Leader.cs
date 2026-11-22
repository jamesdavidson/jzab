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
using System.Linq;

namespace NZab
{
    /**
     * Leader.
     */
    internal class Leader : Participant
    {
        private readonly ConcurrentDictionary<string, PeerHandler> quorumMap =
          new ConcurrentDictionary<string, PeerHandler>();

        private readonly Dictionary<string, PeerHandler> pendingPeers =
          new Dictionary<string, PeerHandler>();

        private long establishedEpoch = -1;
        private Zxid pendingCopZxid = null;
        private PreProcessor preProcessor = null;
        private AckProcessor ackProcessor = null;

        private Zxid lastProposedZxid;
        private Zxid lastAckedZxid;
        private Zxid lastCommittedZxid;

        public Leader(ParticipantState participantState,
                      StateMachine stateMachine,
                      ZabConfig config)
            : base(participantState, stateMachine, config)
        {
            this.electedLeader = participantState.GetServerId();
            filter = new LeaderFilter(messageQueue, election, this);
        }

        protected override void ChangePhase(Phase phase)
        {
            this.currentPhase = phase;
            if (phase == Phase.DISCOVERING)
            {
                if (stateChangeCallback != null)
                {
                    stateChangeCallback.LeaderDiscovering(this.serverId);
                }
                if (failCallback != null)
                {
                    failCallback.LeaderDiscovering();
                }
            }
            else if (phase == Phase.SYNCHRONIZING)
            {
                if (stateChangeCallback != null)
                {
                    stateChangeCallback.LeaderSynchronizing(persistence.GetProposedEpoch());
                }
                if (failCallback != null)
                {
                    failCallback.LeaderSynchronizing();
                }
            }
            else if (phase == Phase.BROADCASTING)
            {
                if (failCallback != null)
                {
                    failCallback.LeaderBroadcasting();
                }
                if (stateChangeCallback != null)
                {
                    stateChangeCallback.LeaderBroadcasting(persistence.GetAckEpoch(),
                                                           GetAllTxns(),
                                                           persistence.GetLastSeenConfig());
                }
            }
            else if (phase == Phase.FINALIZING)
            {
                stateMachine.Recovering(pendings);
                if (persistence.IsInStateTransfer())
                {
                    persistence.UndoStateTransfer();
                }
                foreach (PeerHandler ph in this.quorumMap.Values)
                {
                    ph.Shutdown();
                    PeerHandler removed;
                    this.quorumMap.TryRemove(ph.GetServerId(), out removed);
                }
            }
        }

        public override void Join(string peer)
        {
            try
            {
                List<string> peers = new List<string>();
                peers.Add(this.serverId);
                ClusterConfiguration cnf = new ClusterConfiguration(new Zxid(0, 0), peers, this.serverId);
                persistence.SetLastSeenConfig(cnf);
                byte[] cop = cnf.ToByteArray();
                Transaction txn = new Transaction(cnf.GetVersion(), 1 /* COP_VALUE */, cop);
                persistence.GetLog().Append(txn);
                persistence.SetProposedEpoch(0);
                persistence.SetAckEpoch(0);

                this.election.SpecifyLeader(this.serverId);
                ChangePhase(Phase.BROADCASTING);
                Broadcasting();
            }
            finally
            {
                ChangePhase(Phase.FINALIZING);
            }
        }

        public void Lead()
        {
            try
            {
                ChangePhase(Phase.DISCOVERING);
                WaitProposedEpochFromQuorum();
                ProposeNewEpoch();
                WaitEpochAckFromQuorum();
                string peerId = SelectSyncHistoryOwner();

                ChangePhase(Phase.SYNCHRONIZING);
                if (!peerId.Equals(this.serverId))
                {
                    SynchronizeFromFollower(peerId);
                }
                persistence.SetAckEpoch(persistence.GetProposedEpoch());
                long st = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
                BeginSynchronizing();
                WaitNewLeaderAckFromQuorum();
                long syncTime = (DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond) - st;
                AdjustSyncTimeout((int)syncTime);

                BroadcastCommitMessage();
                RestoreFromSnapshot();
                DeliverUndeliveredTxns();

                ChangePhase(Phase.BROADCASTING);
                foreach (PeerHandler ph in this.quorumMap.Values)
                {
                    ph.StartBroadcastingTask();
                    ph.UpdateHeartbeatTime();
                }
                Broadcasting();
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

        public int GetQuorumSize()
        {
            return persistence.GetLastSeenConfig().GetQuorumSize();
        }

        void WaitProposedEpochFromQuorum()
        {
            ClusterConfiguration currentConfig = persistence.GetLastSeenConfig();
            long acknowledgedEpoch = persistence.GetAckEpoch();
            while (this.quorumMap.Count < GetQuorumSize() - 1)
            {
                MessageTuple tuple = filter.GetExpectedMessage(Message.MessageType.PROPOSED_EPOCH, null, config.GetTimeoutMs());
                Message msg = tuple.GetMessage();
                string source = tuple.GetServerId();
                var epoch = msg.GetProposedEpoch();
                ClusterConfiguration peerConfig = ClusterConfiguration.FromProto(epoch.GetConfig(), source);
                long peerProposedEpoch = epoch.GetProposedEpoch();
                long peerAckedEpoch = epoch.GetCurrentEpoch();
                int syncTimeoutMs = epoch.GetSyncTimeout();
                Zxid peerVersion = peerConfig.GetVersion();
                Zxid selfVersion = currentConfig.GetVersion();

                if (!peerVersion.Equals(selfVersion))
                {
                    if (peerAckedEpoch > acknowledgedEpoch ||
                        (peerAckedEpoch == acknowledgedEpoch && peerVersion.CompareTo(selfVersion) > 0))
                    {
                        throw new BackToElectionException();
                    }
                }
                if (!currentConfig.Contains(source)) continue;
                if (this.quorumMap.ContainsKey(source)) throw new Exception("Quorum set already contains source.");

                PeerHandler ph = new PeerHandler(source, transport, config.GetTimeoutMs() / 3);
                ph.SetLastProposedEpoch(peerProposedEpoch);
                ph.SetSyncTimeoutMs(syncTimeoutMs);
                this.quorumMap.TryAdd(source, ph);
            }
        }

        void ProposeNewEpoch()
        {
            long maxEpoch = persistence.GetProposedEpoch();
            int maxSyncTimeoutMs = GetSyncTimeoutMs();
            foreach (PeerHandler ph in this.quorumMap.Values)
            {
                if (ph.GetLastProposedEpoch() > maxEpoch) maxEpoch = ph.GetLastProposedEpoch();
                if (ph.GetSyncTimeoutMs() > maxSyncTimeoutMs) maxSyncTimeoutMs = ph.GetSyncTimeoutMs();
            }
            long newEpoch = maxEpoch + 1;
            persistence.SetProposedEpoch(newEpoch);
            SetSyncTimeoutMs(maxSyncTimeoutMs);
            Broadcast(this.quorumMap.Keys.GetEnumerator(), MessageBuilder.BuildNewEpochMessage(newEpoch, GetSyncTimeoutMs()));
        }

        void Broadcast(IEnumerator<string> peers, Message message)
        {
            List<string> peerList = new List<string>();
            while (peers.MoveNext()) peerList.Add(peers.Current);
            transport.Broadcast(peerList, message);
        }

        void WaitEpochAckFromQuorum()
        {
            int ackCount = 0;
            while (ackCount < this.quorumMap.Count)
            {
                MessageTuple tuple = filter.GetExpectedMessage(Message.MessageType.ACK_EPOCH, null, config.GetTimeoutMs());
                string source = tuple.GetServerId();
                if (!this.quorumMap.ContainsKey(source)) continue;
                ackCount++;
                var ackEpoch = tuple.GetMessage().GetAckEpoch();
                PeerHandler ph = this.quorumMap[source];
                ph.SetLastAckedEpoch(ackEpoch.GetAcknowledgedEpoch());
                ph.SetLastZxid(MessageBuilder.FromProtoZxid(ackEpoch.GetLastZxid()));
            }
        }

        string SelectSyncHistoryOwner()
        {
            long ackEpoch = persistence.GetAckEpoch();
            Zxid zxid = persistence.GetLatestZxid();
            string peerId = this.serverId;
            foreach (var entry in this.quorumMap)
            {
                long fEpoch = entry.Value.GetLastAckedEpoch();
                Zxid fZxid = entry.Value.GetLastZxid();
                if (fEpoch > ackEpoch || (fEpoch == ackEpoch && fZxid.CompareTo(zxid) > 0))
                {
                    ackEpoch = fEpoch;
                    zxid = fZxid;
                    peerId = entry.Key;
                }
            }
            if (this.stateChangeCallback != null)
            {
                this.stateChangeCallback.InitialHistoryOwner(peerId, ackEpoch, zxid);
            }
            return peerId;
        }

        void SynchronizeFromFollower(string peerId)
        {
            Zxid lastZxid = persistence.GetLatestZxid();
            Message pullTxn = MessageBuilder.BuildPullTxnReq(lastZxid);
            SendMessage(peerId, pullTxn);
            WaitForSync(peerId);
        }

        void WaitNewLeaderAckFromQuorum()
        {
            int completeCount = 0;
            Zxid lastZxid = persistence.GetLatestZxid();
            while (completeCount < this.quorumMap.Count)
            {
                MessageTuple tuple = filter.GetExpectedMessage(Message.MessageType.ACK, null, GetSyncTimeoutMs());
                string source = tuple.GetServerId();
                Zxid zxid = MessageBuilder.FromProtoZxid(tuple.GetMessage().GetAck().GetZxid());
                if (!this.quorumMap.ContainsKey(source)) continue;
                if (zxid.CompareTo(lastZxid) != 0) throw new Exception("Follower not correctly synchronized.");
                PeerHandler ph = this.quorumMap[source];
                ph.SetLastAckedZxid(zxid);
                completeCount++;
            }
        }

        void BroadcastCommitMessage()
        {
            Zxid zxid = persistence.GetLatestZxid();
            Message commit = MessageBuilder.BuildCommit(zxid);
            Broadcast(this.quorumMap.Keys.GetEnumerator(), commit);
            foreach (PeerHandler ph in this.quorumMap.Values)
            {
                ph.SetLastCommittedZxid(zxid);
            }
        }

        void BeginSynchronizing()
        {
            Zxid lastZxid = persistence.GetLatestZxid();
            ClusterConfiguration clusterConfig = persistence.GetLastSeenConfig();
            long proposedEpoch = persistence.GetProposedEpoch();
            foreach (PeerHandler ph in this.quorumMap.Values)
            {
                ph.SetSyncTask(new SyncPeerTask(this, ph.GetServerId(), ph.GetLastZxid(), lastZxid, clusterConfig), proposedEpoch);
                ph.StartSynchronizingTask();
            }
        }

        void BroadcastingInit()
        {
            Zxid lastZxid = persistence.GetLatestZxid();
            this.establishedEpoch = persistence.GetAckEpoch();
            clusterConfig = persistence.GetLastSeenConfig();
            PeerHandler lh = new PeerHandler(serverId, transport, config.GetTimeoutMs() / 3);
            lh.SetLastAckedZxid(lastZxid);
            lh.SetLastCommittedZxid(lastZxid);
            lh.StartBroadcastingTask();
            quorumMap.TryAdd(this.serverId, lh);
            this.preProcessor = new PreProcessor(stateMachine, quorumMap, clusterConfig);
            this.ackProcessor = new AckProcessor(quorumMap, clusterConfig);
            this.syncProcessor = new SyncProposalProcessor(persistence, transport, maxBatchSize);
            this.commitProcessor = new CommitProcessor(stateMachine, lastDeliveredZxid, serverId, transport, new HashSet<string>(quorumMap.Keys), clusterConfig, electedLeader, pendings);
            this.snapProcessor = new SnapshotProcessor(stateMachine, persistence, serverId, transport);

            stateMachine.Leading(new HashSet<string>(quorumMap.Keys), new HashSet<string>(clusterConfig.GetPeers()));
            this.lastCommittedZxid = lastZxid;
            this.lastProposedZxid = lastZxid;
            this.lastAckedZxid = lastZxid;
        }

        void Broadcasting()
        {
            BroadcastingInit();
            try
            {
                while (this.quorumMap.Count >= clusterConfig.GetQuorumSize())
                {
                    MessageTuple tuple = filter.GetMessage(config.GetTimeoutMs());
                    Message msg = tuple.GetMessage();
                    string source = tuple.GetServerId();
                    if (msg.GetType() == Message.MessageType.DISCONNECTED)
                    {
                        string peerId = msg.GetDisconnected().GetServerId();
                        if (quorumMap.ContainsKey(peerId)) OnDisconnected(tuple);
                        else this.transport.Clear(peerId);
                        continue;
                    }
                    if (!quorumMap.ContainsKey(source))
                    {
                        HandleMessageOutsideQuorum(tuple);
                    }
                    else
                    {
                        HandleMessageFromQuorum(tuple);
                        PeerHandler ph = quorumMap[source];
                        ph.UpdateHeartbeatTime();
                        CheckFollowerLiveness();
                    }
                }
            }
            finally
            {
                ackProcessor.Shutdown();
                preProcessor.Shutdown();
                commitProcessor.Shutdown();
                syncProcessor.Shutdown();
                snapProcessor.Shutdown();
                this.lastDeliveredZxid = commitProcessor.GetLastDeliveredZxid();
                this.participantState.UpdateLastDeliveredZxid(this.lastDeliveredZxid);
            }
        }

        void HandleMessageOutsideQuorum(MessageTuple tuple)
        {
            Message msg = tuple.GetMessage();
            string source = tuple.GetServerId();
            if (msg.GetType() == Message.MessageType.PROPOSED_EPOCH)
            {
                ClusterConfiguration cnf = persistence.GetLastSeenConfig();
                if (!cnf.Contains(source)) return;
                int syncTimeoutMs = msg.GetProposedEpoch().GetSyncTimeout();
                if (syncTimeoutMs > GetSyncTimeoutMs()) SetSyncTimeoutMs(syncTimeoutMs);
                Message newEpoch = MessageBuilder.BuildNewEpochMessage(establishedEpoch, GetSyncTimeoutMs());
                SendMessage(source, newEpoch);
            }
            else if (msg.GetType() == Message.MessageType.ACK_EPOCH)
            {
                tuple.SetZxid(lastProposedZxid);
                OnAckEpoch(tuple);
            }
            else if (msg.GetType() == Message.MessageType.QUERY_LEADER)
            {
                Message reply = MessageBuilder.BuildQueryReply(this.serverId);
                SendMessage(source, reply);
            }
            else if (msg.GetType() == Message.MessageType.SYNC_HISTORY)
            {
                OnSyncHistory(tuple);
            }
            else if (msg.GetType() == Message.MessageType.ELECTION_INFO)
            {
                this.election.Reply(tuple);
            }
        }

        void HandleMessageFromQuorum(MessageTuple tuple)
        {
            Message msg = tuple.GetMessage();
            string source = tuple.GetServerId();
            if (msg.GetType() == Message.MessageType.ACK) OnAck(tuple);
            else if (msg.GetType() == Message.MessageType.REQUEST)
            {
                Zxid proposedZxid = GetNextProposedZxid();
                quorumMap[source].SetLastProposedZxid(proposedZxid);
                tuple.SetZxid(proposedZxid);
                preProcessor.ProcessRequest(tuple);
            }
            else if (msg.GetType() == Message.MessageType.FLUSH_REQ) OnFlushReq(tuple);
            else if (msg.GetType() == Message.MessageType.FLUSH) OnFlush(tuple);
            else if (msg.GetType() == Message.MessageType.PROPOSAL) OnProposal(tuple);
            else if (msg.GetType() == Message.MessageType.COMMIT) OnCommit(tuple);
            else if (msg.GetType() == Message.MessageType.REMOVE)
            {
                if (pendingCopZxid != null) return;
                pendingCopZxid = GetNextProposedZxid();
                tuple.SetZxid(pendingCopZxid);
                OnRemove(tuple);
            }
            else if (msg.GetType() == Message.MessageType.DELIVERED) OnDelivered(msg);
            else if (msg.GetType() == Message.MessageType.JOIN)
            {
                if (pendingCopZxid != null) return;
                pendingCopZxid = GetNextProposedZxid();
                tuple.SetZxid(pendingCopZxid);
                OnJoin(tuple);
            }
            else if (msg.GetType() == Message.MessageType.SNAPSHOT) snapProcessor.ProcessRequest(tuple);
            else if (msg.GetType() == Message.MessageType.SNAPSHOT_DONE) commitProcessor.ProcessRequest(tuple);
        }

        void OnJoin(MessageTuple tuple)
        {
            Zxid lastZxid = MessageBuilder.FromProtoZxid(tuple.GetMessage().GetJoin().GetLastZxid());
            string source = tuple.GetServerId();
            PeerHandler ph = new PeerHandler(source, transport, config.GetTimeoutMs() / 3);
            ph.SetLastZxid(lastZxid);
            ph.SetLastSyncedZxid(tuple.GetZxid());
            clusterConfig.AddPeer(source);
            quorumMap.TryAdd(source, ph);
            pendingPeers.Add(source, ph);
            preProcessor.ProcessRequest(tuple);
            ackProcessor.ProcessRequest(tuple);
            commitProcessor.ProcessRequest(tuple);
        }

        void OnCommit(MessageTuple tuple)
        {
            this.lastCommittedZxid = MessageBuilder.FromProtoZxid(tuple.GetMessage().GetCommit().GetZxid());
            if (pendingCopZxid != null && lastCommittedZxid.CompareTo(this.pendingCopZxid) >= 0)
            {
                pendingCopZxid = null;
                if (stateChangeCallback != null) stateChangeCallback.CommitCop();
            }
            if (pendingPeers.Count > 0)
            {
                var keys = pendingPeers.Keys.ToList();
                foreach (var key in keys)
                {
                    PeerHandler ph = pendingPeers[key];
                    Zxid ackZxid = ph.GetLastAckedZxid();
                    if (ackZxid != null && lastCommittedZxid.CompareTo(ackZxid) >= 0)
                    {
                        Message commit = MessageBuilder.BuildCommit(ackZxid);
                        SendMessage(ph.GetServerId(), commit);
                        ph.SetLastCommittedZxid(ackZxid);
                        ph.StartBroadcastingTask();
                        pendingPeers.Remove(key);
                    }
                }
            }
            commitProcessor.ProcessRequest(tuple);
        }

        void OnDisconnected(MessageTuple tuple)
        {
            string peerId = tuple.GetMessage().GetDisconnected().GetServerId();
            pendingPeers.Remove(peerId);
            PeerHandler ph;
            if (this.quorumMap.TryGetValue(peerId, out ph))
            {
                ph.DisableSending();
                ph.Shutdown();
                quorumMap.TryRemove(peerId, out ph);
            }
            preProcessor.ProcessRequest(tuple);
            ackProcessor.ProcessRequest(tuple);
            commitProcessor.ProcessRequest(tuple);
        }

        Zxid OnAck(MessageTuple tuple)
        {
            string source = tuple.GetServerId();
            Zxid ackZxid = MessageBuilder.FromProtoZxid(tuple.GetMessage().GetAck().GetZxid());
            PeerHandler peer;
            if (pendingPeers.TryGetValue(source, out peer))
            {
                peer.SetLastAckedZxid(ackZxid);
                if (lastCommittedZxid.CompareTo(ackZxid) >= 0)
                {
                    Message commit = MessageBuilder.BuildCommit(ackZxid);
                    SendMessage(peer.GetServerId(), commit);
                    peer.SetLastCommittedZxid(ackZxid);
                    peer.StartBroadcastingTask();
                    pendingPeers.Remove(source);
                }
            }
            if (source.Equals(this.serverId))
            {
                this.lastAckedZxid = ackZxid;
                foreach (var ph in pendingPeers.Values)
                {
                    if (!ph.IsSyncStarted() && ackZxid.CompareTo(ph.GetLastSyncedZxid()) >= 0)
                    {
                        ClusterConfiguration cnf = persistence.GetLastConfigWithin(ph.GetLastSyncedZxid());
                        ph.SetSyncTask(new SyncPeerTask(this, ph.GetServerId(), ph.GetLastZxid(), ph.GetLastSyncedZxid(), cnf), establishedEpoch);
                        ph.StartSynchronizingTask();
                    }
                }
            }
            ackProcessor.ProcessRequest(tuple);
            return ackZxid;
        }

        void OnAckEpoch(MessageTuple tuple)
        {
            string source = tuple.GetServerId();
            Zxid lastZxidOfSync = tuple.GetZxid();
            Zxid lastPeerZxid = MessageBuilder.FromProtoZxid(tuple.GetMessage().GetAckEpoch().GetLastZxid());
            PeerHandler ph = new PeerHandler(source, transport, config.GetTimeoutMs() / 3);
            ph.SetLastZxid(lastPeerZxid);
            ph.SetLastSyncedZxid(lastZxidOfSync);
            this.quorumMap.TryAdd(source, ph);
            this.pendingPeers.Add(source, ph);
            preProcessor.ProcessRequest(tuple);
            ackProcessor.ProcessRequest(tuple);
            commitProcessor.ProcessRequest(tuple);
            if (lastAckedZxid.CompareTo(lastZxidOfSync) >= 0)
            {
                ClusterConfiguration cnf = persistence.GetLastConfigWithin(ph.GetLastSyncedZxid());
                ph.SetSyncTask(new SyncPeerTask(this, ph.GetServerId(), ph.GetLastZxid(), ph.GetLastSyncedZxid(), cnf), this.establishedEpoch);
                ph.StartSynchronizingTask();
            }
        }

        void OnRemove(MessageTuple tuple)
        {
            string peerId = tuple.GetMessage().GetRemove().GetServerId();
            clusterConfig.RemovePeer(peerId);
            preProcessor.ProcessRequest(tuple);
            ackProcessor.ProcessRequest(tuple);
        }

        void OnFlushReq(MessageTuple tuple)
        {
            string source = tuple.GetServerId();
            PeerHandler ph = quorumMap[source];
            Zxid zxid = ph.GetLastProposedZxid();
            byte[] body = tuple.GetMessage().GetFlushRequest().GetBody();
            Message msg = MessageBuilder.BuildFlush(zxid, body);
            SendMessage(source, msg);
        }

        void OnSyncHistory(MessageTuple tuple)
        {
            string source = tuple.GetServerId();
            PeerHandler ph = new PeerHandler(source, transport, config.GetTimeoutMs() / 3);
            Zxid lastZxid = MessageBuilder.FromProtoZxid(tuple.GetMessage().GetSyncHistory().GetLastZxid());
            ph.SetLastZxid(lastZxid);
            ph.SetLastSyncedZxid(this.lastAckedZxid);
            ClusterConfiguration cnf = persistence.GetLastConfigWithin(ph.GetLastSyncedZxid());
            ph.SetSyncTask(new SyncPeerTask(this, ph.GetServerId(), ph.GetLastZxid(), ph.GetLastSyncedZxid(), cnf), establishedEpoch);
            Message reply = MessageBuilder.BuildSyncHistoryReply(GetSyncTimeoutMs());
            SendMessage(source, reply);
            ph.StartSynchronizingTask();
            this.quorumMap.TryAdd(source, ph);
        }

        void CheckFollowerLiveness()
        {
            long currentTime = DateTime.Now.Ticks * 100; // 1 tick = 100ns
            foreach (PeerHandler ph in this.quorumMap.Values)
            {
                if (ph.GetServerId().Equals(this.serverId)) continue;
                if (ph.IsDisconnected()) continue;
                long timeoutNs = (ph.IsSynchronizing() ? GetSyncTimeoutMs() : this.config.GetTimeoutMs()) * 1000000L;
                if (currentTime - ph.GetLastHeartbeatTime() >= timeoutNs)
                {
                    string peerId = ph.GetServerId();
                    Message disconnected = MessageBuilder.BuildDisconnected(peerId);
                    this.messageQueue.Add(new MessageTuple(this.serverId, disconnected));
                    ph.MarkDisconnected();
                    if (ph.IsSynchronizing()) IncSyncTimeout();
                }
            }
        }

        private Zxid GetNextProposedZxid()
        {
            if (lastProposedZxid.GetEpoch() != establishedEpoch)
            {
                lastProposedZxid = new Zxid(establishedEpoch, -1);
            }
            lastProposedZxid = new Zxid(establishedEpoch, lastProposedZxid.GetXid() + 1);
            return lastProposedZxid;
        }

        class LeaderFilter : ElectionMessageFilter
        {
            private readonly Leader leader;
            public LeaderFilter(BlockingCollection<MessageTuple> msgQueue, Election election, Leader leader)
                : base(msgQueue, election)
            {
                this.leader = leader;
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
                        if (leader.quorumMap.ContainsKey(peerId))
                        {
                            if (leader.currentPhase != Phase.BROADCASTING) throw new BackToElectionException();
                            return tuple;
                        }
                        else
                        {
                            leader.transport.Clear(peerId);
                        }
                    }
                    else return tuple;
                }
            }
        }
    }
}
