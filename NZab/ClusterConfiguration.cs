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
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;

namespace NZab
{
    /**
     * Configurations(ensemble of cluster, serverId, version).
     */
    internal class ClusterConfiguration : ICloneable
    {
        private Zxid version;
        private readonly HashSet<string> peers;
        private readonly string serverId;

        public ClusterConfiguration(Zxid version,
                                    IEnumerable<string> peers,
                                    string serverId)
        {
            this.version = version;
            this.peers = new HashSet<string>(peers);
            this.serverId = serverId;
        }

        public Zxid GetVersion()
        {
            return this.version;
        }

        public void SetVersion(Zxid newVersion)
        {
            this.version = newVersion;
        }

        public ISet<string> GetPeers()
        {
            return this.peers;
        }

        public string GetServerId()
        {
            return this.serverId;
        }

        public void AddPeer(string peer)
        {
            peers.Add(peer);
        }

        public void RemovePeer(string peer)
        {
            this.peers.Remove(peer);
        }

        public NameValueCollection ToProperties()
        {
            NameValueCollection prop = new NameValueCollection();
            StringBuilder strBuilder = new StringBuilder();
            string strVersion = this.version.ToSimpleString();
            if (this.peers.Count > 0)
            {
                strBuilder.Append(string.Join(",", this.peers));
            }
            prop.Add("peers", strBuilder.ToString());
            prop.Add("version", strVersion);
            prop.Add("serverId", this.serverId);
            return prop;
        }

        public static ClusterConfiguration FromProperties(NameValueCollection prop)
        {
            string strPeers = prop["peers"];
            Zxid version = Zxid.FromSimpleString(prop["version"]);
            string serverId = prop["serverId"];
            HashSet<string> peerSet;
            if (string.IsNullOrEmpty(strPeers))
            {
                peerSet = new HashSet<string>();
            }
            else
            {
                peerSet = new HashSet<string>(strPeers.Split(','));
            }
            return new ClusterConfiguration(version, peerSet, serverId);
        }

        // Proto methods omitted as they depend on the specific proto implementation
        /*
        public static ClusterConfiguration FromProto(...)
        public ZabMessage.ClusterConfiguration ToProto()
        public byte[] ToByteArray()
        public static ClusterConfiguration FromByteArray(byte[] buffer, string serverId)
        public Transaction ToTransaction()
        */

        public bool Contains(string peerId)
        {
            return this.peers.Contains(peerId);
        }

        public override string ToString()
        {
            var props = ToProperties();
            return string.Join(";", props.AllKeys.Select(key => key + "=" + props[key]));
        }

        /**
         * Gets the minimal quorum size.
         */
        public int GetQuorumSize()
        {
            int clusterSize = this.peers.Count;
            if (clusterSize == 0)
            {
                return 0;
            }
            else
            {
                return clusterSize / 2 + 1;
            }
        }

        public object Clone()
        {
            return new ClusterConfiguration(version, new HashSet<string>(peers), serverId);
        }
    }
}
