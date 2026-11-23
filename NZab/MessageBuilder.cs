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

using Microsoft.Extensions.Logging;
using JMessage = jzab.Message;
using JZxid = jzab.Zxid;
using JClusterConfiguration = jzab.ClusterConfiguration;

namespace NZab
{
    /**
     * Helper class used for creating protobuf messages.
     */
    internal static class MessageBuilder
    {
        private static readonly ILogger LOG = LoggerFactory.Create(b =>
        {
            b.AddSimpleConsole();
            b.SetMinimumLevel(LogLevel.Debug);
        }).CreateLogger(typeof(MessageBuilder));

        /**
         * Converts Zxid object to protobuf Zxid object.
         */
        public static JZxid ToProtoZxid(Zxid zxid)
        {
            return new JZxid
            {
                epoch = zxid.GetEpoch(),
                xid = zxid.GetXid()
            };
        }

        /**
         * Converts protobuf Zxid object to Zxid object.
         */
        public static Zxid FromProtoZxid(JZxid zxid)
        {
            return new Zxid(zxid.epoch, zxid.xid);
        }

        /**
         * Converts protobuf Proposal object to Transaction object.
         */
        public static Transaction FromProposal(jzab.Proposal prop)
        {
            Zxid zxid = FromProtoZxid(prop.zxid);
            return new Transaction(zxid, (int)prop.type, prop.body);
        }

        /**
         * Converts Transaction object to protobuf Proposal object.
         */
        public static jzab.Proposal FromTransaction(Transaction txn)
        {
            return new jzab.Proposal
            {
                zxid = ToProtoZxid(txn.GetZxid()),
                body = txn.GetBody(),
                type = (jzab.Proposal.ProposalType)txn.GetTransactionType()
            };
        }

        /**
         * Creates CEPOCH message.
         */
        public static JMessage BuildProposedEpoch(long proposedEpoch,
                                                  long acknowledgedEpoch,
                                                  ClusterConfiguration config,
                                                  int syncTimeoutMs)
        {
            jzab.ProposedEpoch pEpoch = new jzab.ProposedEpoch
            {
                proposed_epoch = proposedEpoch,
                current_epoch = acknowledgedEpoch,
                config = BuildConfig(config),
                sync_timeout = syncTimeoutMs
            };

            return new JMessage
            {
                type = JMessage.MessageType.PROPOSED_EPOCH,
                proposed_epoch = pEpoch
            };
        }

        /**
         * Creates a NEW_EPOCH message.
         */
        public static JMessage BuildNewEpochMessage(long epoch, int syncTimeoutMs)
        {
            jzab.NewEpoch nEpoch = new jzab.NewEpoch
            {
                new_epoch = epoch,
                sync_timeout = syncTimeoutMs
            };

            return new JMessage
            {
                type = JMessage.MessageType.NEW_EPOCH,
                new_epoch = nEpoch
            };
        }

        /**
         * Creates a ACK_EPOCH message.
         */
        public static JMessage BuildAckEpoch(long epoch, Zxid lastZxid)
        {
            jzab.AckEpoch ackEpoch = new jzab.AckEpoch
            {
                acknowledged_epoch = epoch,
                lastZxid = ToProtoZxid(lastZxid)
            };

            return new JMessage
            {
                type = JMessage.MessageType.ACK_EPOCH,
                ack_epoch = ackEpoch
            };
        }

        /**
         * Creates an INVALID message.
         */
        public static JMessage BuildInvalidMessage(byte[] content)
        {
            jzab.InvalidMessage msg = new jzab.InvalidMessage
            {
                received_bytes = content
            };

            return new JMessage
            {
                type = JMessage.MessageType.INVALID_MESSAGE,
                invalid = msg
            };
        }

        /**
         * Creates a PULL_TXN_REQ message.
         */
        public static JMessage BuildPullTxnReq(Zxid lastZxid)
        {
            jzab.PullTxnReq req = new jzab.PullTxnReq
            {
                last_zxid = ToProtoZxid(lastZxid)
            };

            return new JMessage
            {
                type = JMessage.MessageType.PULL_TXN_REQ,
                pull_txn_req = req
            };
        }

        /**
         * Creates a PROPOSAL message.
         */
        public static JMessage BuildProposal(Transaction txn)
        {
            jzab.Proposal prop = FromTransaction(txn);

            return new JMessage
            {
                type = JMessage.MessageType.PROPOSAL,
                proposal = prop
            };
        }

        /**
         * Creates a PROPOSAL message.
         */
        public static JMessage BuildProposal(Transaction txn, string clientId)
        {
            jzab.Proposal prop = FromTransaction(txn);
            prop.clientId = clientId;

            return new JMessage
            {
                type = JMessage.MessageType.PROPOSAL,
                proposal = prop
            };
        }

        /**
         * Creates a DIFF message.
         */
        public static JMessage BuildDiff(Zxid lastZxid)
        {
            jzab.Diff diff = new jzab.Diff
            {
                last_zxid = ToProtoZxid(lastZxid)
            };

            return new JMessage
            {
                type = JMessage.MessageType.DIFF,
                diff = diff
            };
        }

        /**
         * Creates a TRUNCATE message.
         */
        public static JMessage BuildTruncate(Zxid lastPrefixZxid, Zxid lastZxid)
        {
            jzab.Truncate trunc = new jzab.Truncate
            {
                last_prefix_zxid = ToProtoZxid(lastPrefixZxid),
                last_zxid = ToProtoZxid(lastZxid)
            };

            return new JMessage
            {
                type = JMessage.MessageType.TRUNCATE,
                truncate = trunc
            };
        }

        /**
         * Creates a NEW_LEADER message.
         */
        public static JMessage BuildNewLeader(long epoch)
        {
            jzab.NewLeader nl = new jzab.NewLeader
            {
                epoch = epoch
            };

            return new JMessage
            {
                type = JMessage.MessageType.NEW_LEADER,
                new_leader = nl
            };
        }

        /**
         * Creates an ACK message.
         */
        public static JMessage BuildAck(Zxid zxid)
        {
            jzab.Ack ack = new jzab.Ack
            {
                zxid = ToProtoZxid(zxid)
            };

            return new JMessage
            {
                type = JMessage.MessageType.ACK,
                ack = ack
            };
        }

        /**
         * Creates a SNAPSHOT message.
         */
        public static JMessage BuildSnapshot(Zxid lastZxid)
        {
            jzab.Snapshot snapshot = new jzab.Snapshot
            {
                last_zxid = ToProtoZxid(lastZxid)
            };

            return new JMessage
            {
                type = JMessage.MessageType.SNAPSHOT,
                snapshot = snapshot
            };
        }

        /**
         * Creates a SNAPSHOT message.
         */
        public static JMessage BuildSnapshot(Zxid lastZxid, Zxid snapZxid)
        {
            jzab.Snapshot snapshot = new jzab.Snapshot
            {
                last_zxid = ToProtoZxid(lastZxid),
                snap_zxid = ToProtoZxid(snapZxid)
            };

            return new JMessage
            {
                type = JMessage.MessageType.SNAPSHOT,
                snapshot = snapshot
            };
        }

        /**
         * Creates a REQUEST message.
         */
        public static JMessage BuildRequest(byte[] request)
        {
            jzab.Request req = new jzab.Request
            {
                request = request
            };

            return new JMessage
            {
                type = JMessage.MessageType.REQUEST,
                request = req
            };
        }

        /**
         * Creates a COMMIT message.
         */
        public static JMessage BuildCommit(Zxid zxid)
        {
            jzab.Commit commit = new jzab.Commit
            {
                zxid = ToProtoZxid(zxid)
            };

            return new JMessage
            {
                type = JMessage.MessageType.COMMIT,
                commit = commit
            };
        }

        /**
         * Creates a HANDSHAKE message.
         */
        public static JMessage BuildHandshake(string nodeId)
        {
            jzab.Handshake handshake = new jzab.Handshake
            {
                node_id = nodeId
            };

            return new JMessage
            {
                type = JMessage.MessageType.HANDSHAKE,
                handshake = handshake
            };
        }

        /**
         * Creates a HEARTBEAT message.
         */
        public static JMessage BuildHeartbeat()
        {
            return new JMessage
            {
                type = JMessage.MessageType.HEARTBEAT
            };
        }

        /**
         * Creates a DISCONNECTED message.
         */
        public static JMessage BuildDisconnected(string serverId)
        {
            jzab.Disconnected dis = new jzab.Disconnected
            {
                server_id = serverId
            };

            return new JMessage
            {
                type = JMessage.MessageType.DISCONNECTED,
                disconnected = dis
            };
        }

        /**
         * Creates a QUERY_LEADER message.
         */
        public static JMessage BuildQueryLeader()
        {
            return new JMessage
            {
                type = JMessage.MessageType.QUERY_LEADER
            };
        }

        /**
         * Creates a QUERY_REPLY message.
         */
        public static JMessage BuildQueryReply(string leader)
        {
            jzab.QueryReply reply = new jzab.QueryReply
            {
                leader = leader
            };

            return new JMessage
            {
                type = JMessage.MessageType.QUERY_LEADER_REPLY,
                reply = reply
            };
        }

        /**
         * Creates a JOIN message.
         */
        public static JMessage BuildJoin(Zxid lastZxid)
        {
            jzab.Join join = new jzab.Join
            {
                lastZxid = ToProtoZxid(lastZxid)
            };

            return new JMessage
            {
                type = JMessage.MessageType.JOIN,
                join = join
            };
        }

        /**
         * Creates a protobuf ClusterConfiguration object.
         */
        public static JClusterConfiguration BuildConfig(ClusterConfiguration config)
        {
            JClusterConfiguration zConfig = new JClusterConfiguration
            {
                version = ToProtoZxid(config.GetVersion())
            };

            foreach (string peer in config.GetPeers())
            {
                zConfig.servers.Add(peer);
            }

            return zConfig;
        }

        /**
         * Converts protobuf ClusterConfiguration object to ClusterConfiguration object.
         */
        public static ClusterConfiguration FromProtoConfig(JClusterConfiguration config, string serverId)
        {
            return new ClusterConfiguration(FromProtoZxid(config.version), config.servers, serverId);
        }

        /**
         * Creates a SYNC_END message.
         */
        public static JMessage BuildSyncEnd(ClusterConfiguration config)
        {
            return new JMessage
            {
                type = JMessage.MessageType.SYNC_END,
                config = BuildConfig(config)
            };
        }

        /**
         * Creates a REMOVE message.
         */
        public static JMessage BuildRemove(string serverId)
        {
            jzab.Remove remove = new jzab.Remove
            {
                serverId = serverId
            };

            return new JMessage
            {
                type = JMessage.MessageType.REMOVE,
                remove = remove
            };
        }

        /**
         * Creates a SHUT_DOWN message.
         */
        public static JMessage BuildShutDown()
        {
            return new JMessage
            {
                type = JMessage.MessageType.SHUT_DOWN
            };
        }

        /**
         * Creates a DELIVERED message.
         */
        public static JMessage BuildDelivered(Zxid deliveredZxid)
        {
            jzab.Delivered delivered = new jzab.Delivered
            {
                zxid = ToProtoZxid(deliveredZxid)
            };

            return new JMessage
            {
                type = JMessage.MessageType.DELIVERED,
                delivered = delivered
            };
        }

        /**
         * Creates a FILE_HEADER message.
         */
        public static JMessage BuildFileHeader(long length)
        {
            jzab.FileHeader header = new jzab.FileHeader
            {
                length = length
            };

            return new JMessage
            {
                type = JMessage.MessageType.FILE_HEADER,
                file_header = header
            };
        }

        /**
         * Creates a FILE_RECEIVED message.
         */
        public static JMessage BuildFileReceived(string fullPath)
        {
            jzab.FileReceived received = new jzab.FileReceived
            {
                full_path = fullPath
            };

            return new JMessage
            {
                type = JMessage.MessageType.FILE_RECEIVED,
                file_received = received
            };
        }

        /**
         * Creates a FLUSH_REQ message.
         */
        public static JMessage BuildFlushRequest(byte[] body)
        {
            jzab.FlushRequest flushReq = new jzab.FlushRequest
            {
                body = body
            };

            return new JMessage
            {
                type = JMessage.MessageType.FLUSH_REQ,
                flush_request = flushReq
            };
        }

        /**
         * Creates a FLUSH message.
         */
        public static JMessage BuildFlush(Zxid lastProposedZxid, byte[] body)
        {
            jzab.Flush flush = new jzab.Flush
            {
                zxid = ToProtoZxid(lastProposedZxid),
                body = body
            };

            return new JMessage
            {
                type = JMessage.MessageType.FLUSH,
                flush = flush
            };
        }

        /**
         * Creates a SYNC_HISTORY message.
         */
        public static JMessage BuildSyncHistory(Zxid lastZxid)
        {
            jzab.SyncHistory sync = new jzab.SyncHistory
            {
                last_zxid = ToProtoZxid(lastZxid)
            };

            return new JMessage
            {
                type = JMessage.MessageType.SYNC_HISTORY,
                sync_history = sync
            };
        }

        /**
         * Creates the SYNC_HISTORY_REPLY message.
         */
        public static JMessage BuildSyncHistoryReply(int timeout)
        {
            jzab.SyncHistoryReply reply = new jzab.SyncHistoryReply
            {
                sync_timeout = timeout
            };

            return new JMessage
            {
                type = JMessage.MessageType.SYNC_HISTORY_REPLY,
                sync_history_reply = reply
            };
        }

        /**
         * Creates the ELECTION_INFO message.
         */
        public static JMessage BuildElectionInfo(string vote,
                                                 Zxid lastZxid,
                                                 long ackEpoch,
                                                 long round,
                                                 bool electing)
        {
            jzab.ElectionInfo ei = new jzab.ElectionInfo
            {
                vote = vote,
                zxid = ToProtoZxid(lastZxid),
                ack_epoch = ackEpoch,
                is_electing = electing,
                round = round
            };

            return new JMessage
            {
                type = JMessage.MessageType.ELECTION_INFO,
                election_info = ei
            };
        }

        /**
         * Creates the SNAPSHOT_DONE message.
         */
        public static JMessage BuildSnapshotDone(string filePath)
        {
            jzab.SnapshotDone done = new jzab.SnapshotDone
            {
                file_path = filePath
            };

            return new JMessage
            {
                type = JMessage.MessageType.SNAPSHOT_DONE,
                snapshot_done = done
            };
        }
    }
}