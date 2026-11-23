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
using jzab;

using Microsoft.Extensions.Logging;

namespace NZab
{
    /**
     * Participant is the base class for different roles of Jzab(Leader/Follower).
     */
    internal abstract class Participant
    {
        private static readonly ILogger LOG = LoggerFactory.Create(builder =>
        {
            builder.AddSimpleConsole();
            builder.SetMinimumLevel(LogLevel.Debug);
        }).CreateLogger(typeof(Participant));

        protected readonly Transport transport;
        protected readonly PersistentState persistence;
        protected readonly StateMachine stateMachine;
        protected readonly string serverId;
        protected readonly BlockingCollection<MessageTuple> messageQueue;
        protected Zxid lastDeliveredZxid;
        protected string electedLeader;
        protected readonly ZabConfig config;
        protected Zab.FailureCaseCallback failCallback = null;
        protected Zab.StateChangeCallback stateChangeCallback = null;
        protected readonly ParticipantState participantState;
        protected readonly int maxBatchSize;
        protected readonly Election election;
        protected Phase currentPhase = Phase.DISCOVERING;
        protected readonly PendingRequests pendings = new PendingRequests();

        protected SyncProposalProcessor syncProcessor = null;
        protected CommitProcessor commitProcessor = null;
        protected SnapshotProcessor snapProcessor = null;
        protected ClusterConfiguration clusterConfig = null;
        protected MessageQueueFilter filter;

        protected enum Phase
        {
            DISCOVERING,
            SYNCHRONIZING,
            BROADCASTING,
            FINALIZING
        }

        internal class BackToElectionException : Exception { }

        internal class LeftCluster : Exception
        {
            public LeftCluster(string desc) : base(desc) { }
        }

        internal class JoinFailure : Exception
        {
            public JoinFailure(string desc) : base(desc) { }
        }

        public Participant(ParticipantState participantState,
                           StateMachine stateMachine,
                           ZabConfig config)
        {
            this.participantState = participantState;
            this.transport = participantState.GetTransport();
            this.persistence = participantState.GetPersistence();
            this.lastDeliveredZxid = participantState.GetLastDeliveredZxid();
            this.serverId = participantState.GetServerId();
            this.messageQueue = participantState.GetMessageQueue();
            this.stateMachine = stateMachine;
            this.stateChangeCallback = participantState.GetStateChangeCallback();
            this.failCallback = participantState.GetFailureCaseCallback();
            this.config = config;
            this.election = participantState.GetElection();
            this.maxBatchSize = config.GetMaxBatchSize();
        }

        public virtual void Send(byte[] request, object ctx)
        {
            lock (this)
            {
                if (this.currentPhase != Phase.BROADCASTING)
                {
                    throw new ZabException.InvalidPhase("Zab.send() called while recovering");
                }
                if (pendings.PendingSends.Count > ZabConfig.MAX_PENDING_REQS)
                {
                    throw new ZabException.TooManyPendingRequests();
                }
                pendings.PendingSends.Add(new PendingRequests.Tuple(request, ctx));
                Message msg = MessageBuilder.BuildRequest(request);
                Console.WriteLine($"electedLeader: {electedLeader} on serverId {serverId}");
                SendMessage(this.electedLeader, msg);
            }
        }

        public virtual void Remove(string peerId, object ctx)
        {
            lock (this)
            {
                if (this.currentPhase != Phase.BROADCASTING)
                {
                    throw new ZabException.InvalidPhase("Zab.remove() called while recovering");
                }
                if (pendings.PendingRemoves.Count > 0)
                {
                    throw new ZabException.TooManyPendingRequests("Snapshot already in progress");
                }
                pendings.PendingRemoves.Add(new PendingRequests.Tuple(peerId, ctx));
                Message msg = MessageBuilder.BuildRemove(peerId);
                SendMessage(this.electedLeader, msg);
            }
        }

        public virtual void Flush(byte[] request, object ctx)
        {
            lock (this)
            {
                if (this.currentPhase != Phase.BROADCASTING)
                {
                    throw new ZabException.InvalidPhase("Zab.flush() called while recovering");
                }
                if (pendings.PendingFlushes.Count > ZabConfig.MAX_PENDING_REQS)
                {
                    throw new ZabException.TooManyPendingRequests();
                }
                pendings.PendingFlushes.Add(new PendingRequests.Tuple(request, ctx));
                Message msg = MessageBuilder.BuildFlushRequest(request);
                SendMessage(this.electedLeader, msg);
            }
        }

        public virtual void TakeSnapshot(object ctx)
        {
            lock (this)
            {
                if (this.currentPhase != Phase.BROADCASTING)
                {
                    throw new ZabException.InvalidPhase("Zab.takeSnapshot() called while recovering");
                }
                if (pendings.PendingSnapshots.Count > 0)
                {
                    throw new ZabException.TooManyPendingRequests("A pending snapshot is in progress");
                }
                pendings.PendingSnapshots.Add(ctx);
                Message msg = MessageBuilder.BuildSnapshot(this.lastDeliveredZxid);
                SendMessage(this.serverId, msg);
            }
        }

        public abstract void Join(string peer);

        protected void SendMessage(string dest, Message message)
        {
            this.transport.Send(dest, message);
        }

        protected void WaitForSync(string peer)
        {
            ILog log = this.persistence.GetLog();
            Zxid lastZxid = persistence.GetLatestZxid();
            Zxid lastZxidPeer = null;
            Message msg = null;
            string source = null;

            while (true)
            {
                MessageTuple tuple = filter.GetMessage(GetSyncTimeoutMs());
                source = tuple.GetServerId();
                msg = tuple.GetMessage();
                if ((msg.type != Message.MessageType.DIFF &&
                     msg.type != Message.MessageType.TRUNCATE &&
                     msg.type != Message.MessageType.SNAPSHOT &&
                     msg.type != Message.MessageType.PULL_TXN_REQ) ||
                    !source.Equals(peer))
                {
                    continue;
                }
                else
                {
                    break;
                }
            }

            if (msg.type == Message.MessageType.PULL_TXN_REQ)
            {
                lastZxidPeer = MessageBuilder.FromProtoZxid(msg.pull_txn_req.last_zxid);
                SyncPeerTask syncTask = new SyncPeerTask(this, source, lastZxidPeer, lastZxid, this.persistence.GetLastSeenConfig());
                syncTask.Run();
                MessageTuple tuple = filter.GetExpectedMessage(Message.MessageType.DIFF, peer, GetSyncTimeoutMs());
                msg = tuple.GetMessage();
                lastZxidPeer = MessageBuilder.FromProtoZxid(msg.diff.last_zxid);
                if (lastZxidPeer.CompareTo(lastZxid) != 0)
                {
                    throw new Exception("Expecting leader and follower have same history.");
                }
                WaitForSyncEnd(peer);
                return;
            }

            if (msg.type == Message.MessageType.DIFF)
            {
                lastZxidPeer = MessageBuilder.FromProtoZxid(msg.diff.last_zxid);
                if (lastZxid.CompareTo(lastZxidPeer) == 0)
                {
                    WaitForSyncEnd(peer);
                    return;
                }
            }
            else if (msg.type == Message.MessageType.TRUNCATE)
            {
                Zxid lastPrefixZxid = MessageBuilder.FromProtoZxid(msg.truncate.last_prefix_zxid);
                lastZxidPeer = MessageBuilder.FromProtoZxid(msg.truncate.last_zxid);
                if (lastZxidPeer.Equals(Zxid.ZXID_NOT_EXIST))
                {
                    persistence.BeginStateTransfer();
                }
                else
                {
                    log.Truncate(lastPrefixZxid);
                }
                if (lastZxidPeer.CompareTo(lastPrefixZxid) == 0)
                {
                    WaitForSyncEnd(peer);
                    return;
                }
            }
            else
            {
                lastZxidPeer = MessageBuilder.FromProtoZxid(msg.snapshot.last_zxid);
                Zxid snapZxid = MessageBuilder.FromProtoZxid(msg.snapshot.snap_zxid);
                msg = filter.GetExpectedMessage(Message.MessageType.FILE_RECEIVED, peer, GetSyncTimeoutMs()).GetMessage();
                string filePath = msg.file_received.full_path;
                persistence.BeginStateTransfer();
                persistence.SetSnapshotFile(filePath, snapZxid);
                log.Truncate(Zxid.ZXID_NOT_EXIST);
                if (lastZxidPeer.CompareTo(snapZxid) == 0)
                {
                    WaitForSyncEnd(peer);
                    return;
                }
            }

            log = persistence.GetLog();
            while (true)
            {
                MessageTuple tuple = filter.GetExpectedMessage(Message.MessageType.PROPOSAL, peer, GetSyncTimeoutMs());
                msg = tuple.GetMessage();
                source = tuple.GetServerId();
                var prop = msg.proposal;
                Zxid zxid = MessageBuilder.FromProtoZxid(prop.zxid);
                log.Append(MessageBuilder.FromProposal(prop));
                if (zxid.CompareTo(lastZxidPeer) == 0)
                {
                    WaitForSyncEnd(peer);
                    return;
                }
            }
        }

        void WaitForSyncEnd(string peerId)
        {
            MessageTuple tuple = filter.GetExpectedMessage(Message.MessageType.SYNC_END, peerId, GetSyncTimeoutMs());
            ClusterConfiguration cnf = MessageBuilder.FromProtoConfig(tuple.GetMessage().config, this.serverId);
            this.persistence.SetLastSeenConfig(cnf);
            if (persistence.IsInStateTransfer())
            {
                persistence.EndStateTransfer();
            }
            persistence.CleanupClusterConfigFiles();
        }

        public List<Transaction> GetAllTxns()
        {
            ILog log = this.persistence.GetLog();
            List<Transaction> txns = new List<Transaction>();
            using (ILogIterator iter = log.GetIterator(new Zxid(0, 0)))
            {
                while (iter.HasNext())
                {
                    txns.Add(iter.Next());
                }
                return txns;
            }
        }

        public void RestoreFromSnapshot()
        {
            Zxid snapshotZxid = persistence.GetSnapshotZxid();
            if (snapshotZxid != Zxid.ZXID_NOT_EXIST && lastDeliveredZxid == Zxid.ZXID_NOT_EXIST)
            {
                string snapshot = persistence.GetSnapshotFile();
                using (FileStream fin = new FileStream(snapshot, FileMode.Open, FileAccess.Read))
                {
                    stateMachine.Restore(fin);
                    lastDeliveredZxid = snapshotZxid;
                }
            }
        }

        public void DeliverUndeliveredTxns()
        {
            Zxid startZxid = new Zxid(lastDeliveredZxid.GetEpoch(), lastDeliveredZxid.GetXid() + 1);
            ILog log = persistence.GetLog();
            using (ILogIterator iter = log.GetIterator(startZxid))
            {
                while (iter.HasNext())
                {
                    Transaction txn = iter.Next();
                    if (txn.GetTransactionType() == 0 /* USER_REQUEST_VALUE */)
                    {
                        stateMachine.Deliver(txn.GetZxid(), txn.GetBody(), null, null);
                    }
                    lastDeliveredZxid = txn.GetZxid();
                }
            }
        }

        protected abstract void ChangePhase(Phase phase);

        protected void OnDelivered(Message msg)
        {
            this.lastDeliveredZxid = MessageBuilder.FromProtoZxid(msg.delivered.zxid);
        }

        protected void OnFlush(MessageTuple tuple)
        {
            commitProcessor.ProcessRequest(tuple);
        }

        protected void OnProposal(MessageTuple tuple)
        {
            syncProcessor.ProcessRequest(tuple);
            commitProcessor.ProcessRequest(tuple);
        }

        protected void ClearMessageQueue()
        {
            MessageTuple tuple;
            while (messageQueue.TryTake(out tuple))
            {
                Message msg = tuple.GetMessage();
                if (msg.type == Message.MessageType.DISCONNECTED)
                {
                    this.transport.Clear(msg.disconnected.server_id);
                }
                else if (msg.type == Message.MessageType.SHUT_DOWN)
                {
                    throw new LeftCluster("Shutdown Zab.");
                }
            }
        }

        protected int GetSyncTimeoutMs()
        {
            return participantState.GetSyncTimeoutMs();
        }

        protected int IncSyncTimeout()
        {
            int syncTimeoutMs = GetSyncTimeoutMs();
            participantState.SetSyncTimeoutMs(syncTimeoutMs * 2);
            return syncTimeoutMs;
        }

        protected void SetSyncTimeoutMs(int timeout)
        {
            participantState.SetSyncTimeoutMs(timeout);
        }

        protected void AdjustSyncTimeout(int timeoutMs)
        {
            int timeoutSec = (timeoutMs + 999) / 1000;
            for (int i = 0; i < 32; ++i)
            {
                if ((1 << i) >= timeoutSec)
                {
                    SetSyncTimeoutMs((1 << i) * 1000);
                    return;
                }
            }
            throw new Exception("Timeout is too large!");
        }

        internal class SyncPeerTask
        {
            private readonly Participant outer;
            private readonly string peerId;
            private readonly Zxid peerLatestZxid;
            private readonly Zxid lastSyncZxid;
            private readonly ClusterConfiguration clusterConfig;
            private bool stop = false;

            public SyncPeerTask(Participant outer, string peerId, Zxid peerLatestZxid, Zxid lastSyncZxid, ClusterConfiguration config)
            {
                this.outer = outer;
                this.peerId = peerId;
                this.peerLatestZxid = peerLatestZxid;
                this.lastSyncZxid = lastSyncZxid;
                this.clusterConfig = config;
            }

            public Zxid GetLastSyncZxid() { return this.lastSyncZxid; }
            public void Stop() { this.stop = true; }

            void StateTransfer(string snapFile, Zxid snapZxid, ILog log)
            {
                Zxid syncPoint = Zxid.ZXID_NOT_EXIST;
                if (snapFile != null)
                {
                    Message snapshot = MessageBuilder.BuildSnapshot(lastSyncZxid, snapZxid);
                    outer.transport.Send(peerId, snapshot);
                    outer.transport.Send(peerId, snapFile);
                    syncPoint = new Zxid(snapZxid.GetEpoch(), snapZxid.GetXid() + 1);
                }
                else
                {
                    Message truncate = MessageBuilder.BuildTruncate(Zxid.ZXID_NOT_EXIST, lastSyncZxid);
                    outer.SendMessage(peerId, truncate);
                }
                using (ILogIterator iter = log.GetIterator(syncPoint))
                {
                    while (iter.HasNext())
                    {
                        if (stop) return;
                        Transaction txn = iter.Next();
                        if (txn.GetZxid().CompareTo(this.lastSyncZxid) > 0) break;
                        Message prop = MessageBuilder.BuildProposal(txn);
                        outer.SendMessage(peerId, prop);
                    }
                }
            }

            void SyncFromLog(ILog log)
            {
                DivergingTuple dp = log.FirstDivergingPoint(peerLatestZxid);
                Zxid zxid = dp.GetDivergingZxid();
                if (zxid.CompareTo(peerLatestZxid) == 0)
                {
                    Message diff = MessageBuilder.BuildDiff(lastSyncZxid);
                    outer.SendMessage(peerId, diff);
                }
                else
                {
                    Message trunc = MessageBuilder.BuildTruncate(zxid, lastSyncZxid);
                    outer.SendMessage(peerId, trunc);
                }
                using (ILogIterator iter = dp.GetIterator())
                {
                    while (iter.HasNext())
                    {
                        if (stop) return;
                        Transaction txn = iter.Next();
                        Message prop = MessageBuilder.BuildProposal(txn);
                        outer.SendMessage(peerId, prop);
                    }
                }
            }

            public void Run()
            {
                ILog log = outer.persistence.GetLog();
                Zxid snapZxid = outer.persistence.GetSnapshotZxid();
                string snapFile = outer.persistence.GetSnapshotFile();

                if (peerLatestZxid.Equals(lastSyncZxid))
                {
                    Message diff = MessageBuilder.BuildDiff(lastSyncZxid);
                    outer.SendMessage(peerId, diff);
                }
                else if (peerLatestZxid.CompareTo(lastSyncZxid) > 0)
                {
                    if (peerLatestZxid.GetEpoch() == lastSyncZxid.GetEpoch())
                    {
                        Message trunc = MessageBuilder.BuildTruncate(lastSyncZxid, lastSyncZxid);
                        outer.SendMessage(peerId, trunc);
                    }
                    else
                    {
                        StateTransfer(snapFile, snapZxid, log);
                    }
                }
                else
                {
                    if (snapFile != null && snapZxid.CompareTo(peerLatestZxid) >= 0)
                    {
                        StateTransfer(snapFile, snapZxid, log);
                    }
                    else
                    {
                        SyncFromLog(log);
                    }
                }
                if (this.stop) return;
                Message syncEnd = MessageBuilder.BuildSyncEnd(this.clusterConfig);
                outer.SendMessage(peerId, syncEnd);
            }
        }
    }
}
