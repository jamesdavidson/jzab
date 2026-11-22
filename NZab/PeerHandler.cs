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
     * Handles peer connection.
     */
    internal class PeerHandler
    {
        protected long lastProposedEpoch = -1;
        protected long lastAckedEpoch = -1;
        protected long lastHeartbeatTimeNs;
        protected Zxid lastZxid = null;
        protected Zxid lastAckedZxid = null;
        protected readonly string serverId;
        protected readonly BlockingCollection<Message> broadcastingQueue = new BlockingCollection<Message>();
        protected Participant.SyncPeerTask syncTask = null;
        protected long newleaderEpoch;
        protected readonly Transport transport;
        protected readonly int heartbeatIntervalMs;
        private bool disableSending = false;
        protected Task ftSync = null;
        protected Task ftBroad = null;
        private Zxid lastSyncedZxid = null;
        private Zxid lastProposedZxid = Zxid.ZXID_NOT_EXIST;
        private int syncTimeoutMs = 0;
        private Zxid lastCommittedZxid = Zxid.ZXID_NOT_EXIST;
        private bool disconnected = false;

        public PeerHandler(string serverId, Transport transport, int heartbeatIntervalMs)
        {
            this.serverId = serverId;
            this.transport = transport;
            this.heartbeatIntervalMs = heartbeatIntervalMs;
            UpdateHeartbeatTime();
        }

        public Zxid GetLastSyncedZxid() { return this.lastSyncedZxid; }
        public void SetLastSyncedZxid(Zxid zxid) { this.lastSyncedZxid = zxid; }
        public string GetServerId() { return this.serverId; }
        public long GetLastHeartbeatTime() { return this.lastHeartbeatTimeNs; }
        public void UpdateHeartbeatTime() { this.lastHeartbeatTimeNs = DateTime.Now.Ticks * 100; }
        public long GetLastProposedEpoch() { return this.lastProposedEpoch; }
        public void SetLastProposedEpoch(long epoch) { this.lastProposedEpoch = epoch; }
        public long GetLastAckedEpoch() { return this.lastAckedEpoch; }
        public void SetLastAckedEpoch(long epoch) { this.lastAckedEpoch = epoch; }
        public Zxid GetLastZxid() { return this.lastZxid; }
        public void SetLastZxid(Zxid zxid) { this.lastZxid = zxid; }
        public void SetLastAckedZxid(Zxid zxid) { this.lastAckedZxid = zxid; }
        public Zxid GetLastAckedZxid() { return this.lastAckedZxid; }
        public void SetLastCommittedZxid(Zxid zxid) { this.lastCommittedZxid = zxid; }
        public Zxid GetLastCommittedZxid() { return this.lastCommittedZxid; }
        public Zxid GetLastProposedZxid() { return this.lastProposedZxid; }
        public void SetLastProposedZxid(Zxid zxid) { this.lastProposedZxid = zxid; }

        public void DisableSending() { lock (this) { this.disableSending = true; } }

        public bool IsSyncStarted() { return this.ftSync != null; }
        public bool IsSynchronizing() { return this.ftSync != null && this.ftBroad == null; }
        public void SetSyncTimeoutMs(int timeout) { this.syncTimeoutMs = timeout; }
        public int GetSyncTimeoutMs() { return this.syncTimeoutMs; }
        public void MarkDisconnected() { this.disconnected = true; }
        public bool IsDisconnected() { return this.disconnected; }

        public void QueueMessage(Message msg) { this.broadcastingQueue.Add(msg); }

        public void SetSyncTask(Participant.SyncPeerTask task, long newLeaderEpoch)
        {
            this.syncTask = task;
            this.newleaderEpoch = newLeaderEpoch;
        }

        public void SendMessage(Message msg)
        {
            lock (this)
            {
                if (!this.disableSending)
                {
                    this.transport.Send(this.serverId, msg);
                }
            }
        }

        public void Shutdown()
        {
            if (this.ftSync != null)
            {
                this.syncTask.Stop();
                this.ftSync.Wait();
            }
            if (this.ftBroad != null)
            {
                this.broadcastingQueue.Add(MessageBuilder.BuildShutDown());
                this.ftBroad.Wait();
            }
            this.transport.Clear(this.serverId);
        }

        public void StartSynchronizingTask()
        {
            if (this.syncTask == null) throw new Exception("SyncTask is null!");
            if (this.ftSync != null) throw new Exception("SyncTask can be only started once!");
            if (this.ftBroad != null) throw new Exception("SyncTask must be started before broadcasting task");
            this.ftSync = Task.Factory.StartNew(SyncFollower, TaskCreationOptions.LongRunning);
        }

        public void StartBroadcastingTask()
        {
            if (this.ftBroad != null) throw new Exception("Broadcast task can be only started once!");
            this.ftBroad = Task.Factory.StartNew(BroadcastingFollower, TaskCreationOptions.LongRunning);
        }

        private void SyncFollower()
        {
            try
            {
                syncTask.Run();
                Message nl = MessageBuilder.BuildNewLeader(newleaderEpoch);
                SendMessage(nl);
            }
            catch (Exception) { }
        }

        private void BroadcastingFollower()
        {
            Message heartbeat = MessageBuilder.BuildHeartbeat();
            while (true)
            {
                Message msg;
                if (!broadcastingQueue.TryTake(out msg, heartbeatIntervalMs))
                {
                    SendMessage(heartbeat);
                    continue;
                }
                if (msg.type == Message.MessageType.SHUT_DOWN) return;
                SendMessage(msg);
            }
        }
    }
}
