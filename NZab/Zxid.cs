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
using System.IO;
using System.Net;
using Microsoft.Extensions.Logging;

namespace NZab
{
    /**
     * Simple implementation of Zxid.
     */
    public class Zxid : IComparable<Zxid>
    {
        private static readonly ILogger LOG = LoggerFactory.Create(b =>
        {
            b.AddSimpleConsole();
            b.SetMinimumLevel(LogLevel.Debug);
        }).CreateLogger(typeof(Zxid));

        private readonly long epoch;
        private readonly long xid;
        private const int ZXID_LENGTH = 16;
        public static readonly Zxid ZXID_NOT_EXIST = new Zxid(0, -1);

        public Zxid(long epoch, long xid)
        {
            this.epoch = epoch;
            this.xid = xid;
        }

        public static int GetZxidLength()
        {
            return ZXID_LENGTH;
        }

        public long GetEpoch()
        {
            return this.epoch;
        }

        public long GetXid()
        {
            return this.xid;
        }

        public static Zxid FromByteArray(byte[] bytes)
        {
            using (MemoryStream ms = new MemoryStream(bytes))
            using (BinaryReader reader = new BinaryReader(ms))
            {
                // Java's DataInputStream reads in big-endian
                long epoch = IPAddress.NetworkToHostOrder(reader.ReadInt64());
                long xid = IPAddress.NetworkToHostOrder(reader.ReadInt64());
                return new Zxid(epoch, xid);
            }
        }

        /**
         * Serialize this Zxid into a fixed size (16 bytes) byte array.
         *
         * The resulting byte array can be deserialized with fromByteArray.
         * @return an array of bytes.
         */
        public byte[] ToByteArray()
        {
            using (MemoryStream ms = new MemoryStream())
            using (BinaryWriter writer = new BinaryWriter(ms))
            {
                // Java's DataOutputStream writes in big-endian
                writer.Write(IPAddress.HostToNetworkOrder(this.epoch));
                writer.Write(IPAddress.HostToNetworkOrder(this.xid));
                writer.Flush();
                return ms.ToArray();
            }
        }

        public int CompareTo(Zxid zxid)
        {
            if (zxid == null) return 1;
            if (this.epoch != zxid.epoch)
            {
                return this.epoch.CompareTo(zxid.epoch);
            }
            return this.xid.CompareTo(zxid.xid);
        }

        public override string ToString()
        {
            return string.Format("Zxid [epoch: {0}, xid: {1}]", this.epoch, this.xid);
        }

        public override bool Equals(object o)
        {
            if (o == null || !(o is Zxid))
            {
                return false;
            }
            Zxid z = (Zxid)o;
            return CompareTo(z) == 0;
        }

        public override int GetHashCode()
        {
            // The Java version returns 0, which is inefficient but we'll follow it for now if requested,
            // though usually we'd want something better.
            return 0;
        }

        public string ToSimpleString()
        {
            return string.Format("{0:D15}_{1:D15}", this.epoch, this.xid);
        }

        public static Zxid FromSimpleString(string zxid)
        {
            string[] str = zxid.Split('_');
            if (str.Length != 2)
            {
                throw new Exception("Can't convert string to zxid, wrong format.");
            }
            return new Zxid(long.Parse(str[0]), long.Parse(str[1]));
        }

        public static bool operator ==(Zxid left, Zxid right)
        {
            if (ReferenceEquals(left, null))
            {
                return ReferenceEquals(right, null);
            }
            return left.Equals(right);
        }

        public static bool operator !=(Zxid left, Zxid right)
        {
            return !(left == right);
        }
    }
}
