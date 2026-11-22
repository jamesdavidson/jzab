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
using jzab;

namespace NZab
{
    /**
     * Zab is a fault-tolerant, replicated protocol that guarantees all requests
     * submitted to it will be delivered in same order to all servers in the
     * cluster. The Zab class exposes all the operations of Jzab library.
     */
    public class Zab
    {
        /**
         * Task for background "main" thread.
         */
        private readonly Task<object> task;

        /**
         * Server Id for Zab.
         */
        private string serverId;

        /**
         * Configuration for Zab.
         */
        private readonly ZabConfig config;

        /**
         * StateMachine callback.
         */
        private readonly StateMachine stateMachine;

        /**
         * Background thread for Zab.
         */
        private readonly MainThread mainThread;

        /**
         * Constructs a Zab instance by recovering from the log directory.
         *
         * @param stateMachine the state machine implementation of application.
         * @param config the configuration for Jzab, see ZabConfig.
         */
        public Zab(StateMachine stateMachine, ZabConfig config)
            : this(stateMachine, config, null, null, null, null, null, null)
        {
        }

        /**
         * Constructs a Zab instance by joining an existing cluster.
         */
        public Zab(StateMachine stateMachine, ZabConfig config, string serverId,
                   string joinPeer)
            : this(stateMachine, config, serverId, joinPeer, null, null, null, null)
        {
        }

        /**
         * Constructs a Zab instance by booting from static cluster configuration.
         */
        public Zab(StateMachine stateMachine, ZabConfig config, string serverId,
                   ISet<string> peers)
            : this(stateMachine, config, serverId, null, peers, null, null, null)
        {
        }

        internal Zab(StateMachine stateMachine,
                     ZabConfig config,
                     string serverId,
                     string joinPeer,
                     ISet<string> peers,
                     PersistentState initState,
                     StateChangeCallback stateCallback,
                     FailureCaseCallback failureCallback)
        {
            this.config = config;
            this.stateMachine = stateMachine;
            this.serverId = serverId;
            try
            {
                // Initialize.
                this.mainThread = new MainThread(this, joinPeer,
                                                 peers,
                                                 stateCallback,
                                                 failureCallback,
                                                 initState);
            }
            catch (Exception e)
            {
                throw new Exception("Failed to initialize Zab.", e);
            }
            // Starts main thread.
            this.task = Task.Factory.StartNew<object>(() => this.mainThread.Run(), TaskCreationOptions.LongRunning);
        }

        /**
         * Get the task of the background working thread of Zab.
         *
         * @return the task object of MainThread.
         */
        public Task<object> GetTask()
        {
            return this.task;
        }

        public void Send(byte[] request, object ctx)
        {
            this.mainThread.Send(request, ctx);
        }

        public void Flush(byte[] request, object ctx)
        {
            this.mainThread.Flush(request, ctx);
        }

        public void Remove(string peerId, object ctx)
        {
            this.mainThread.Remove(peerId, ctx);
        }

        public void TakeSnapshot(object ctx)
        {
            this.mainThread.TakeSnapshot(ctx);
        }

        public void Shutdown()
        {
            this.mainThread.Shutdown();
        }

        public string GetServerId()
        {
            return this.serverId;
        }

        public interface StateChangeCallback
        {
            void Electing();
            void LeaderDiscovering(string electedLeader);
            void FollowerDiscovering(string electedLeader);
            void InitialHistoryOwner(string server, long aEpoch, Zxid zxid);
            void LeaderSynchronizing(long epoch);
            void FollowerSynchronizing(long epoch);
            void LeaderBroadcasting(long epoch, List<Transaction> history,
                                    ClusterConfiguration config);
            void FollowerBroadcasting(long epoch, List<Transaction> history,
                                      ClusterConfiguration config);
            void LeftCluster();
            void CommitCop();
        }

        public class SimulatedException : Exception
        {
            public SimulatedException(string desc) : base(desc) { }
            public SimulatedException() { }
        }

        public abstract class FailureCaseCallback
        {
            public virtual void LeaderDiscovering() { }
            public virtual void FollowerDiscovering() { }
            public virtual void LeaderSynchronizing() { }
            public virtual void FollowerSynchronizing() { }
            public virtual void LeaderBroadcasting() { }
            public virtual void FollowerBroadcasting() { }
        }

        internal class MainThread : Transport.Receiver
        {
            private readonly Zab outer;
            private ParticipantState participantState;
            private readonly BlockingCollection<MessageTuple> messageQueue =
              new BlockingCollection<MessageTuple>();
            private readonly string joinPeer;
            private readonly StateChangeCallback stateChangeCallback;
            private Transport transport;
            private Election election;
            private readonly PersistentState persistence;
            private Participant participant = null;

            internal MainThread(Zab outer,
                               string joinPeer,
                               ISet<string> peers,
                               StateChangeCallback stateChangeCallback,
                               FailureCaseCallback failureCallback,
                               PersistentState initState)
            {
                this.outer = outer;
                this.joinPeer = joinPeer;
                this.stateChangeCallback = stateChangeCallback;
                if (initState == null)
                {
                    persistence = new PersistentState(outer.config.GetLogDir());
                }
                else
                {
                    persistence = initState;
                }
                if (joinPeer != null)
                {
                    if (!persistence.IsEmpty())
                    {
                        throw new Exception("Log directory must be empty.");
                    }
                }
                else
                {
                    if (outer.serverId != null)
                    {
                        Zxid version = new Zxid(0, -1);
                        ClusterConfiguration cnf =
                          new ClusterConfiguration(version, peers, outer.serverId);
                        persistence.SetLastSeenConfig(cnf);
                    }
                    else
                    {
                        ClusterConfiguration cnf = persistence.GetLastSeenConfig();
                        if (cnf == null)
                        {
                            throw new Exception("Can't find configuration file.");
                        }
                        outer.serverId = cnf.GetServerId();
                        persistence.CleanupClusterConfigFiles();
                    }
                }
                // Creates transport.
                transport = new NettyTransport(
                    outer.serverId,
                    this,
                    persistence.GetLogDir());

                election = new FastLeaderElection(
                    persistence,
                    transport,
                    messageQueue);

                participantState = new ParticipantState(persistence,
                                                        outer.serverId,
                                                        transport,
                                                        messageQueue,
                                                        stateChangeCallback,
                                                        failureCallback,
                                                        outer.config.GetMinSyncTimeoutMs(),
                                                        election);
            }

            public object Run()
            {
                try
                {
                    if (this.joinPeer != null)
                    {
                        Join(this.joinPeer);
                    }
                    while (true)
                    {
                        if (stateChangeCallback != null)
                        {
                            stateChangeCallback.Electing();
                        }
                        string leader = this.election.ElectLeader();
                        if (leader.Equals(outer.serverId))
                        {
                            participant = new Leader(participantState, outer.stateMachine, outer.config);
                            ((Leader)participant).Lead();
                        }
                        else
                        {
                            participant = new Follower(participantState, outer.stateMachine, outer.config);
                            ((Follower)participant).Follow(leader);
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Shutdown
                }
                catch (Participant.LeftCluster)
                {
                    // Shutdown
                }
                catch (Exception)
                {
                    throw;
                }
                finally
                {
                    participantState.GetTransport()?.Shutdown();
                }
                if (stateChangeCallback != null)
                {
                    stateChangeCallback.LeftCluster();
                }
                return null;
            }

            public void OnReceived(string source, Message message)
            {
                MessageTuple tuple = new MessageTuple(source, message);
                this.messageQueue.Add(tuple);
            }

            public void OnDisconnected(string server)
            {
                Message disconnected = MessageBuilder.BuildDisconnected(server);
                this.participantState.EnqueueMessage(new MessageTuple(outer.serverId,
                                                                      disconnected));
            }

            void Join(string peer)
            {
                if (peer.Equals(outer.serverId))
                {
                    participant = new Leader(participantState, outer.stateMachine, outer.config);
                }
                else
                {
                    participant = new Follower(participantState, outer.stateMachine, outer.config);
                }
                participant.Join(peer);
            }

            internal void Send(byte[] buffer, object ctx)
            {
                if (this.participant == null)
                {
                    throw new ZabException.InvalidPhase("Zab.send() called while recovering");
                }
                this.participant.Send(buffer, ctx);
            }

            internal void Remove(string peerId, object ctx)
            {
                if (this.participant == null)
                {
                    throw new ZabException.InvalidPhase("Zab.remove() called while recovering");
                }
                this.participant.Remove(peerId, ctx);
            }

            internal void Flush(byte[] buffer, object ctx)
            {
                if (this.participant == null)
                {
                    throw new ZabException.InvalidPhase("Zab.flush() called while recovering");
                }
                this.participant.Flush(buffer, ctx);
            }

            internal void TakeSnapshot(object ctx)
            {
                if (this.participant == null)
                {
                    throw new ZabException.InvalidPhase("Zab.takeSnapshot() called while recovering");
                }
                this.participant.TakeSnapshot(ctx);
            }

            internal void Shutdown()
            {
                this.participantState.EnqueueShutdown();
                this.outer.task.Wait();
                this.transport?.Shutdown();
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
                        throw new Participant.LeftCluster("Shutdown Zab.");
                    }
                }
            }
        }
    }
}
