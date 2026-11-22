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
using jzab;
using Microsoft.Extensions.Logging;

namespace NZab
{
    /**
     * Fast leader election implementation. Fast leader election will try its best
     * effort to elect the leader with the "best" history to minimize the
     * synchronization cost.
     */
    internal class FastLeaderElection : Election
    {
        private static readonly ILogger LOG =
            LoggerFactory.Create(builder => builder.AddConsole())
                         .CreateLogger<FastLeaderElection>();

        // Fast leader election needs transport to exchange vote information.
        internal readonly Transport transport;

        // The queue for incoming messages(both queries and replies).
        internal readonly BlockingCollection<MessageTuple> messageQueue;

        // The last vote for this server.
        private VoteInfo voteInfo = null;

        // Round number.
        private long round = 0;

        // Persistent state.
        private readonly PersistentState persistence;

        // Message queue filter for fast leader election.
        private readonly ElectioneerFilter filter;

        internal FastLeaderElection(PersistentState persistence,
                                    Transport transport,
                                    BlockingCollection<MessageTuple> messageQueue)
        {
            this.transport = transport;
            this.messageQueue = messageQueue;
            this.persistence = persistence;
            this.filter = new ElectioneerFilter(this, messageQueue);
        }

        public string ElectLeader()
        {
            ClusterConfiguration clusterConfig = persistence.GetLastSeenConfig();
            Zxid lastZxid = persistence.GetLog().GetLatestZxid();
            string serverId = clusterConfig.GetServerId();
            long ackEpoch = persistence.GetAckEpoch();

            // The map stores all the votes from the servers who are in the same round.
            Dictionary<string, VoteInfo> receivedVotes = new Dictionary<string, VoteInfo>();

            // Everytime enters election, increments the round number.
            this.round++;

            // The first vote should be itself.
            this.voteInfo = new VoteInfo(serverId, ackEpoch, lastZxid, round, true);

            int timeoutMs = 100;
            int maxTimeoutMs = 1600;

            // Broadcasts its own vote first.
            Broadcast(clusterConfig);

            while (true)
            {
                MessageTuple msgTuple;
                try
                {
                    msgTuple = filter.GetMessage(timeoutMs);
                }
                catch (TimeoutException)
                {
                    // Timeout without any incoming vote message.
                    if (receivedVotes.Count >= clusterConfig.GetQuorumSize())
                    {
                        // If we've already received votes from a quorum of servers who are in
                        // the same round, then we assume probably we find the server who has
                        // the "best" history.
                        this.voteInfo.electing = false;
                        return this.voteInfo.vote;
                    }

                    // No any incoming message after certain timeout, broadcasts its own
                    // vote and increase its timeout.
                    Broadcast(clusterConfig);
                    timeoutMs = timeoutMs * 2 > maxTimeoutMs ? maxTimeoutMs : 2 * timeoutMs;
                    continue;
                }

                VoteInfo vote = VoteInfo.FromMessage(msgTuple.GetMessage());
                string source = msgTuple.GetServerId();

                if (!clusterConfig.Contains(source))
                {
                    // If the vote comes from a server who is not in your current
                    // configuration, ignores it.
                    LOG.LogDebug(
                        "The vote is from server {Source} who is not in current configuration, ignores it.",
                        source);
                    continue;
                }

                if (vote.electing)
                {
                    // The vote comes from a server who is also in electing phase.
                    if (vote.round > this.voteInfo.round)
                    {
                        LOG.LogDebug(
                            "The round of peer's vote {PeerRound} is larger than itself {OwnRound}",
                            vote.round,
                            this.voteInfo.round);

                        this.round = vote.round;

                        // Updates its round number.
                        this.voteInfo.round = vote.round;

                        // Since the round number has been changed, we need to clear the map.
                        receivedVotes.Clear();

                        if (this.voteInfo.CompareTo(vote) < 0)
                        {
                            // Updates its vote if the peer's vote is better.
                            this.voteInfo = vote;
                        }

                        Broadcast(clusterConfig);
                    }
                    else if (vote.round == this.voteInfo.round &&
                             this.voteInfo.CompareTo(vote) < 0)
                    {
                        // Updates its vote if the peer's vote is better.
                        this.voteInfo = vote;
                        Broadcast(clusterConfig);
                    }
                    else if (vote.round < this.voteInfo.round)
                    {
                        // Ignores if the peer's round is smaller than itself.
                        continue;
                    }
                    else if (vote.round == this.voteInfo.round &&
                             this.voteInfo.CompareTo(vote) > 0)
                    {
                        Broadcast(clusterConfig);
                    }

                    // Updates the received votes.
                    receivedVotes[source] = vote;

                    if (receivedVotes.Count == clusterConfig.GetPeers().Count)
                    {
                        this.voteInfo.electing = false;
                        return this.voteInfo.vote;
                    }
                }
                else
                {
                    // Which means the peer is in non-electing phase.
                    this.voteInfo = vote;
                    this.voteInfo.electing = false;
                    return this.voteInfo.vote;
                }
            }
        }

        public void Reply(MessageTuple tuple)
        {
            if (tuple.GetMessage().election_info.is_electing &&
                this.voteInfo != null)
            {
                // If it's the server first time joining a cluster, it won't
                // initialize its vote information until first synchronization from
                // leader is done. The vote might be null before the synchronization is
                // done. In this case, we won't reply its vote to other querier.
                LOG.LogDebug("Replies to {ServerId} with leader info : {Vote}",
                             tuple.GetServerId(),
                             voteInfo.vote);

                this.transport.Send(tuple.GetServerId(), this.voteInfo.ToMessage());
            }
        }

        public void SpecifyLeader(string leader)
        {
            this.voteInfo = new VoteInfo(leader, -1, Zxid.ZXID_NOT_EXIST, -1, false);
        }

        // Broadcasts its vote to all the peers in current configuration.
        internal void Broadcast(ClusterConfiguration config)
        {
            Message vote = voteInfo.ToMessage();
            foreach (string server in config.GetPeers())
            {
                this.transport.Send(server, vote);
            }
        }

        /**
         * The information of vote.
         */
        internal class VoteInfo : IComparable<VoteInfo>
        {
            internal readonly string vote;
            internal readonly long ackEpoch;
            internal readonly Zxid zxid;
            internal long round;
            internal bool electing;

            internal VoteInfo(string vote,
                              long ackEpoch,
                              Zxid zxid,
                              long round,
                              bool electing)
            {
                this.vote = vote;
                this.ackEpoch = ackEpoch;
                this.zxid = zxid;
                this.round = round;
                this.electing = electing;
            }

            internal Message ToMessage()
            {
                return MessageBuilder.BuildElectionInfo(vote,
                                                        zxid,
                                                        ackEpoch,
                                                        round,
                                                        electing);
            }

            // Compares two votes. The order of the comparison is :
            //  ackEpoch -> lastZxid -> serverId
            public int CompareTo(VoteInfo vi)
            {
                if (ackEpoch != vi.ackEpoch)
                {
                    return this.ackEpoch.CompareTo(vi.ackEpoch);
                }

                if (!this.zxid.Equals(vi.zxid))
                {
                    return this.zxid.CompareTo(vi.zxid);
                }

                return string.CompareOrdinal(this.vote, vi.vote);
            }

            public override bool Equals(object obj)
            {
                if (obj == null || !(obj is VoteInfo))
                {
                    return false;
                }

                return CompareTo((VoteInfo)obj) == 0;
            }

            public override int GetHashCode()
            {
                return 0;
            }

            internal static VoteInfo FromMessage(Message msg)
            {
                ElectionInfo info = msg.election_info;
                return new VoteInfo(info.vote,
                                    info.ack_epoch,
                                    MessageBuilder.FromProtoZxid(info.zxid),
                                    info.round,
                                    info.is_electing);
            }
        }

        /**
         * This filter filters any message except the ELECTION_INFO message.
         */
        private class ElectioneerFilter : MessageQueueFilter
        {
            private readonly FastLeaderElection owner;

            internal ElectioneerFilter(FastLeaderElection owner,
                                       BlockingCollection<MessageTuple> msgQueue)
                : base(msgQueue)
            {
                this.owner = owner;
            }

            public override MessageTuple GetMessage(int timeoutMs)
            {
                long startMs = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;

                while (true)
                {
                    long nowMs = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
                    int remainMs = timeoutMs - (int)(nowMs - startMs);
                    if (remainMs < 0)
                    {
                        remainMs = 0;
                    }

                    MessageTuple tuple = base.GetMessage(remainMs);
                    Message msg = tuple.GetMessage();

                    if (msg.type == Message.MessageType.ELECTION_INFO)
                    {
                        // Got what we want, return it to caller.
                        return tuple;
                    }

                    if (msg.type == Message.MessageType.DISCONNECTED)
                    {
                        owner.transport.Clear(msg.disconnected.server_id);
                    }
                }
            }
        }
    }
}
