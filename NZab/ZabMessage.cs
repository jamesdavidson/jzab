using ProtoBuf;
using System.Collections.Generic;

namespace jzab
{
    [ProtoContract]
    public class Message
    {
        public enum MessageType
        {
            [ProtoEnum(Name = "HEARTBEAT")] HEARTBEAT = 0,
            [ProtoEnum(Name = "PROPOSED_EPOCH")] PROPOSED_EPOCH = 1,
            [ProtoEnum(Name = "NEW_EPOCH")] NEW_EPOCH = 2,
            [ProtoEnum(Name = "ACK_EPOCH")] ACK_EPOCH = 3,
            [ProtoEnum(Name = "NEW_LEADER")] NEW_LEADER = 4,
            [ProtoEnum(Name = "ACK_LEADER")] ACK_LEADER = 5,
            [ProtoEnum(Name = "PROPOSE")] PROPOSE = 7,
            [ProtoEnum(Name = "ACK")] ACK = 8,
            [ProtoEnum(Name = "COMMIT")] COMMIT = 9,
            [ProtoEnum(Name = "PULL_TXN_REQ")] PULL_TXN_REQ = 10,
            [ProtoEnum(Name = "PROPOSAL")] PROPOSAL = 11,
            [ProtoEnum(Name = "DIFF")] DIFF = 12,
            [ProtoEnum(Name = "TRUNCATE")] TRUNCATE = 13,
            [ProtoEnum(Name = "REQUEST")] REQUEST = 15,
            [ProtoEnum(Name = "HANDSHAKE")] HANDSHAKE = 16,
            [ProtoEnum(Name = "DISCONNECTED")] DISCONNECTED = 17,
            [ProtoEnum(Name = "QUERY_LEADER")] QUERY_LEADER = 18,
            [ProtoEnum(Name = "QUERY_LEADER_REPLY")] QUERY_LEADER_REPLY = 19,
            [ProtoEnum(Name = "JOIN")] JOIN = 20,
            [ProtoEnum(Name = "SYNC_END")] SYNC_END = 21,
            [ProtoEnum(Name = "REMOVE")] REMOVE = 22,
            [ProtoEnum(Name = "SHUT_DOWN")] SHUT_DOWN = 23,
            [ProtoEnum(Name = "DELIVERED")] DELIVERED = 24,
            [ProtoEnum(Name = "FILE_HEADER")] FILE_HEADER = 25,
            [ProtoEnum(Name = "FILE_RECEIVED")] FILE_RECEIVED = 26,
            [ProtoEnum(Name = "FLUSH_REQ")] FLUSH_REQ = 27,
            [ProtoEnum(Name = "FLUSH")] FLUSH = 28,
            [ProtoEnum(Name = "SYNC_HISTORY")] SYNC_HISTORY = 29,
            [ProtoEnum(Name = "SYNC_HISTORY_REPLY")] SYNC_HISTORY_REPLY = 30,
            [ProtoEnum(Name = "ELECTION_INFO")] ELECTION_INFO = 31,
            [ProtoEnum(Name = "SNAPSHOT")] SNAPSHOT = 32,
            [ProtoEnum(Name = "SNAPSHOT_DONE")] SNAPSHOT_DONE = 33,
            [ProtoEnum(Name = "INVALID_MESSAGE")] INVALID_MESSAGE = 100,
        }

        [ProtoMember(1)]
        public MessageType type { get; set; }

        [ProtoMember(2)]
        public ProposedEpoch proposed_epoch { get; set; }

        [ProtoMember(3)]
        public NewEpoch new_epoch { get; set; }

        [ProtoMember(4)]
        public AckEpoch ack_epoch { get; set; }

        [ProtoMember(5)]
        public PullTxnReq pull_txn_req { get; set; }

        [ProtoMember(6)]
        public Proposal proposal { get; set; }

        [ProtoMember(7)]
        public Diff diff { get; set; }

        [ProtoMember(8)]
        public Truncate truncate { get; set; }

        [ProtoMember(9)]
        public NewLeader new_leader { get; set; }

        [ProtoMember(10)]
        public Ack ack { get; set; }

        [ProtoMember(12)]
        public Request request { get; set; }

        [ProtoMember(13)]
        public Commit commit { get; set; }

        [ProtoMember(14)]
        public Handshake handshake { get; set; }

        [ProtoMember(15)]
        public Disconnected disconnected { get; set; }

        [ProtoMember(16)]
        public QueryReply reply { get; set; }

        [ProtoMember(17)]
        public ClusterConfiguration config { get; set; }

        [ProtoMember(18)]
        public Remove remove { get; set; }

        [ProtoMember(19)]
        public Delivered delivered { get; set; }

        [ProtoMember(20)]
        public FileHeader file_header { get; set; }

        [ProtoMember(21)]
        public FileReceived file_received { get; set; }

        [ProtoMember(22)]
        public FlushRequest flush_request { get; set; }

        [ProtoMember(23)]
        public Flush flush { get; set; }

        [ProtoMember(24)]
        public Join join { get; set; }

        [ProtoMember(25)]
        public SyncHistory sync_history { get; set; }

        [ProtoMember(26)]
        public SyncHistoryReply sync_history_reply { get; set; }

        [ProtoMember(27)]
        public ElectionInfo election_info { get; set; }

        [ProtoMember(28)]
        public Snapshot snapshot { get; set; }

        [ProtoMember(29)]
        public SnapshotDone snapshot_done { get; set; }

        [ProtoMember(30)]
        public InvalidMessage invalid { get; set; }

        public byte[] ToByteArray()
        {
            using (var ms = new System.IO.MemoryStream())
            {
                Serializer.Serialize(ms, this);
                return ms.ToArray();
            }
        }
    }

    [ProtoContract]
    public class InvalidMessage
    {
        [ProtoMember(1)]
        public byte[] received_bytes { get; set; }
    }

    [ProtoContract]
    public class ProposedEpoch
    {
        [ProtoMember(1)]
        public long proposed_epoch { get; set; }

        [ProtoMember(2)]
        public long current_epoch { get; set; }

        [ProtoMember(3)]
        public ClusterConfiguration config { get; set; }

        [ProtoMember(4)]
        public int sync_timeout { get; set; }
    }

    [ProtoContract]
    public class NewEpoch
    {
        [ProtoMember(1)]
        public long new_epoch { get; set; }

        [ProtoMember(2)]
        public int sync_timeout { get; set; }
    }

    [ProtoContract]
    public class AckEpoch
    {
        [ProtoMember(1)]
        public long acknowledged_epoch { get; set; }

        [ProtoMember(2)]
        public Zxid lastZxid { get; set; }
    }

    [ProtoContract]
    public class PullTxnReq
    {
        [ProtoMember(1)]
        public Zxid last_zxid { get; set; }
    }

    [ProtoContract]
    public class Proposal
    {
        public enum ProposalType
        {
            [ProtoEnum(Name = "USER_REQUEST")] USER_REQUEST = 0,
            [ProtoEnum(Name = "COP")] COP = 1,
        }

        [ProtoMember(1)]
        public Zxid zxid { get; set; }

        [ProtoMember(2)]
        public byte[] body { get; set; }

        [ProtoMember(3)]
        public ProposalType type { get; set; }

        [ProtoMember(4)]
        public string clientId { get; set; }
    }

    [ProtoContract]
    public class Diff
    {
        [ProtoMember(1)]
        public Zxid last_zxid { get; set; }
    }

    [ProtoContract]
    public class Truncate
    {
        [ProtoMember(1)]
        public Zxid last_prefix_zxid { get; set; }

        [ProtoMember(2)]
        public Zxid last_zxid { get; set; }
    }

    [ProtoContract]
    public class Snapshot
    {
        [ProtoMember(1)]
        public Zxid last_zxid { get; set; }

        [ProtoMember(2)]
        public Zxid snap_zxid { get; set; }
    }

    [ProtoContract]
    public class SnapshotDone
    {
        [ProtoMember(1)]
        public string file_path { get; set; }
    }

    [ProtoContract]
    public class Zxid
    {
        [ProtoMember(1)]
        public long epoch { get; set; }

        [ProtoMember(2)]
        public long xid { get; set; }
    }

    [ProtoContract]
    public class NewLeader
    {
        [ProtoMember(1)]
        public long epoch { get; set; }
    }

    [ProtoContract]
    public class Ack
    {
        [ProtoMember(1)]
        public Zxid zxid { get; set; }
    }

    [ProtoContract]
    public class Commit
    {
        [ProtoMember(1)]
        public Zxid zxid { get; set; }
    }

    [ProtoContract]
    public class Request
    {
        [ProtoMember(1)]
        public byte[] request { get; set; }
    }

    [ProtoContract]
    public class Handshake
    {
        [ProtoMember(1)]
        public string node_id { get; set; }
    }

    [ProtoContract]
    public class Disconnected
    {
        [ProtoMember(1)]
        public string server_id { get; set; }
    }

    [ProtoContract]
    public class ClusterConfiguration
    {
        [ProtoMember(1)]
        public List<string> servers { get; set; } = new List<string>();

        [ProtoMember(2)]
        public Zxid version { get; set; }
    }

    [ProtoContract]
    public class QueryReply
    {
        [ProtoMember(1)]
        public string leader { get; set; }
    }

    [ProtoContract]
    public class Join
    {
        [ProtoMember(1)]
        public Zxid lastZxid { get; set; }
    }

    [ProtoContract]
    public class Remove
    {
        [ProtoMember(1)]
        public string serverId { get; set; }
    }

    [ProtoContract]
    public class Delivered
    {
        [ProtoMember(1)]
        public Zxid zxid { get; set; }
    }

    [ProtoContract]
    public class FileHeader
    {
        [ProtoMember(1)]
        public long length { get; set; }
    }

    [ProtoContract]
    public class FileReceived
    {
        [ProtoMember(1)]
        public string full_path { get; set; }
    }

    [ProtoContract]
    public class FlushRequest
    {
        [ProtoMember(2)]
        public byte[] body { get; set; }
    }

    [ProtoContract]
    public class Flush
    {
        [ProtoMember(1)]
        public Zxid zxid { get; set; }

        [ProtoMember(2)]
        public byte[] body { get; set; }
    }

    [ProtoContract]
    public class SyncHistory
    {
        [ProtoMember(1)]
        public Zxid last_zxid { get; set; }
    }

    [ProtoContract]
    public class SyncHistoryReply
    {
        [ProtoMember(1)]
        public int sync_timeout { get; set; }
    }

    [ProtoContract]
    public class ElectionInfo
    {
        [ProtoMember(1)]
        public string vote { get; set; }

        [ProtoMember(2)]
        public Zxid zxid { get; set; }

        [ProtoMember(3)]
        public long ack_epoch { get; set; }

        [ProtoMember(4)]
        public bool is_electing { get; set; }

        [ProtoMember(5)]
        public long round { get; set; }
    }
}
