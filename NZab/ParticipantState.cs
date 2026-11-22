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

using System.Collections.Concurrent;
using jzab;

namespace NZab
{
    /**
     * Participant state. It will be passed across different instances of
     * Leader/Follower class during the lifetime of Zab.
     */
    internal class ParticipantState
    {
        /**
         * Persistent variables for Zab.
         */
        private readonly PersistentState persistence;

        /**
         * The last delivered zxid. It's not persisted to disk. Just a place holder
         * to pass between Follower/Leader.
         */
        private Zxid lastDeliveredZxid = Zxid.ZXID_NOT_EXIST;

        /**
         * Message queue. The receiving callback simply parses the message and puts
         * it in queue, it's up to Leader/Follower/Election to take out
         * and process the message.
         */
        private readonly BlockingCollection<MessageTuple> messageQueue;

        /**
         * Used for communication between nodes.
         */
        private readonly Transport transport;

        /**
         * Server ID of the Participant.
         */
        private readonly string serverId;

        /**
         * The synchronization timeout in milliseconds. It will be adjusted
         * dynamically.
         */
        private int syncTimeoutMs;

        /**
         * The object for leader election.
         */
        private readonly Election election;

        /**
         * Callbacks for state change. Used for testing only.
         */
        private Zab.StateChangeCallback stateChangeCallback = null;

        /**
         * Callbacks for simulating failures. Used for testing only.
         */
        private Zab.FailureCaseCallback failureCallback = null;

        internal ParticipantState(PersistentState persistence,
                                  string serverId,
                                  Transport transport,
                                  BlockingCollection<MessageTuple> messageQueue,
                                  Zab.StateChangeCallback stCallback,
                                  Zab.FailureCaseCallback failureCallback,
                                  int syncTimeoutMs,
                                  Election election)
        {
            this.persistence = persistence;
            this.serverId = serverId;
            this.messageQueue = messageQueue;
            this.stateChangeCallback = stCallback;
            this.failureCallback = failureCallback;
            this.transport = transport;
            this.syncTimeoutMs = syncTimeoutMs;
            this.election = election;
        }

        public BlockingCollection<MessageTuple> GetMessageQueue()
        {
            return this.messageQueue;
        }

        public string GetServerId()
        {
            return this.serverId;
        }

        public Transport GetTransport()
        {
            return this.transport;
        }

        public PersistentState GetPersistence()
        {
            return this.persistence;
        }

        public Zxid GetLastDeliveredZxid()
        {
            return this.lastDeliveredZxid;
        }

        public int GetSyncTimeoutMs()
        {
            return this.syncTimeoutMs;
        }

        public void SetSyncTimeoutMs(int timeoutMs)
        {
            this.syncTimeoutMs = timeoutMs;
        }

        public void UpdateLastDeliveredZxid(Zxid zxid)
        {
            this.lastDeliveredZxid = zxid;
        }

        public Zab.StateChangeCallback GetStateChangeCallback()
        {
            return this.stateChangeCallback;
        }

        public Zab.FailureCaseCallback GetFailureCaseCallback()
        {
            return this.failureCallback;
        }

        public Election GetElection()
        {
            return this.election;
        }

        public void EnqueueShutdown()
        {
            Message shutdown = MessageBuilder.BuildShutDown();
            this.messageQueue.Add(new MessageTuple(this.serverId, shutdown));
        }

        public void EnqueueMessage(MessageTuple tuple)
        {
            this.messageQueue.Add(tuple);
        }
    }
}
