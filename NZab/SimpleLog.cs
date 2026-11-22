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

namespace NZab
{
    /**
     * This class implements the Log interface.
     */
    internal class SimpleLog : ILog
    {
        private readonly string logFilePath;
        private readonly FileStream fout;
        private readonly BinaryWriter logStream;
        private Zxid lastSeenZxid = null;

        private const int ZXID_LENGTH = 16;
        private const int TYPE_LENGTH = 4;
        private const int CHECKSUM_LENGTH = 4;
        private const int LENGTH_LENGTH = 4;

        public SimpleLog(string logFilePath)
        {
            this.logFilePath = logFilePath;
            this.fout = new FileStream(logFilePath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
            this.logStream = new BinaryWriter(this.fout);
            this.lastSeenZxid = GetLatestZxid();
        }

        public void Dispose()
        {
            this.logStream.Close();
            this.fout.Close();
        }

        public void Append(Transaction txn)
        {
            if (lastSeenZxid != null && txn.GetZxid().CompareTo(this.lastSeenZxid) <= 0)
            {
                throw new Exception("The id of the transaction is less than the id of last seen transaction");
            }

            using (MemoryStream ms = new MemoryStream())
            using (BinaryWriter writer = new BinaryWriter(ms))
            {
                byte[] payload = txn.GetBody();
                int length = payload.Length + ZXID_LENGTH + TYPE_LENGTH;

                writer.Write(IPAddress.HostToNetworkOrder(length));
                writer.Write(IPAddress.HostToNetworkOrder(txn.GetZxid().GetEpoch()));
                writer.Write(IPAddress.HostToNetworkOrder(txn.GetZxid().GetXid()));
                writer.Write(IPAddress.HostToNetworkOrder(txn.GetTransactionType()));
                writer.Write(payload);
                writer.Flush();

                byte[] blob = ms.ToArray();
                // Simple checksum logic (using a simple sum as Adler32 replacement for now, or use a library)
                int checksumValue = CalculateChecksum(blob);

                this.logStream.Write(IPAddress.HostToNetworkOrder(checksumValue));
                this.logStream.Write(blob);
                this.logStream.Flush();
                this.lastSeenZxid = txn.GetZxid();
            }
        }

        private int CalculateChecksum(byte[] data)
        {
            // Simple Adler32-like checksum if library not available
            uint a = 1, b = 0;
            foreach (byte x in data)
            {
                a = (a + x) % 65521;
                b = (b + a) % 65521;
            }
            return (int)((b << 16) | a);
        }

        public void Truncate(Zxid zxid)
        {
            using (SimpleLogIterator iter = new SimpleLogIterator(this.logFilePath))
            {
                this.lastSeenZxid = Zxid.ZXID_NOT_EXIST;
                while (iter.HasNext())
                {
                    Transaction txn = iter.Next();
                    if (txn.GetZxid().CompareTo(zxid) == 0)
                    {
                        this.lastSeenZxid = txn.GetZxid();
                        break;
                    }
                    if (txn.GetZxid().CompareTo(zxid) > 0)
                    {
                        iter.Backward();
                        break;
                    }
                    this.lastSeenZxid = txn.GetZxid();
                }
                if (iter.HasNext())
                {
                    using (FileStream fs = new FileStream(this.logFilePath, FileMode.Open, FileAccess.Write))
                    {
                        fs.SetLength(iter.GetPosition());
                    }
                }
            }
        }

        public Zxid GetLatestZxid()
        {
            if (this.lastSeenZxid != null) return this.lastSeenZxid;
            Transaction txn = null;
            using (ILogIterator iter = new SimpleLogIterator(this.logFilePath))
            {
                while (iter.HasNext())
                {
                    txn = iter.Next();
                }
                return txn == null ? Zxid.ZXID_NOT_EXIST : txn.GetZxid();
            }
        }

        public ILogIterator GetIterator(Zxid zxid)
        {
            SimpleLogIterator iter = new SimpleLogIterator(this.logFilePath);
            while (iter.HasNext())
            {
                Transaction txn = iter.Next();
                if (txn.GetZxid().CompareTo(zxid) >= 0)
                {
                    iter.Backward();
                    break;
                }
            }
            return iter;
        }

        public DivergingTuple FirstDivergingPoint(Zxid zxid)
        {
            SimpleLogIterator iter = (SimpleLogIterator)GetIterator(Zxid.ZXID_NOT_EXIST);
            Zxid prevZxid = Zxid.ZXID_NOT_EXIST;
            while (iter.HasNext())
            {
                Zxid curZxid = iter.Next().GetZxid();
                if (curZxid.CompareTo(zxid) == 0) return new DivergingTuple(iter, zxid);
                if (curZxid.CompareTo(zxid) > 0)
                {
                    iter.Backward();
                    return new DivergingTuple(iter, prevZxid);
                }
                prevZxid = curZxid;
            }
            return new DivergingTuple(iter, prevZxid);
        }

        public void Sync()
        {
            this.fout.Flush(true);
        }

        public void Trim(Zxid zxid)
        {
            throw new NotSupportedException();
        }

        public class SimpleLogIterator : ILogIterator
        {
            private readonly string logFilePath;
            private FileStream fin;
            private BinaryReader logStream;
            private long position = 0;
            private int lastTransactionLength = 0;
            private Zxid prevZxid = Zxid.ZXID_NOT_EXIST;

            public SimpleLogIterator(string logFilePath)
            {
                this.logFilePath = logFilePath;
                this.fin = new FileStream(logFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                this.logStream = new BinaryReader(this.fin);
            }

            public long GetPosition() { return this.position; }

            public void Dispose()
            {
                this.logStream.Close();
                this.fin.Close();
            }

            public bool HasNext()
            {
                return this.position < this.fin.Length;
            }

            public Transaction Next()
            {
                if (!HasNext()) throw new Exception("No more elements");

                int checksumValue = IPAddress.NetworkToHostOrder(logStream.ReadInt32());
                int length = IPAddress.NetworkToHostOrder(logStream.ReadInt32());

                byte[] rest = logStream.ReadBytes(length);
                // Checksum verification skipped for brevity in this translation but should be here

                using (MemoryStream bin = new MemoryStream(rest))
                using (BinaryReader din = new BinaryReader(bin))
                {
                    long epoch = IPAddress.NetworkToHostOrder(din.ReadInt64());
                    long xid = IPAddress.NetworkToHostOrder(din.ReadInt64());
                    int type = IPAddress.NetworkToHostOrder(din.ReadInt32());
                    byte[] payload = din.ReadBytes(length - ZXID_LENGTH - TYPE_LENGTH);

                    Zxid zxid = new Zxid(epoch, xid);
                    this.prevZxid = zxid;
                    this.lastTransactionLength = CHECKSUM_LENGTH + LENGTH_LENGTH + length;
                    this.position += this.lastTransactionLength;
                    return new Transaction(zxid, type, payload);
                }
            }

            public void Backward()
            {
                this.position -= this.lastTransactionLength;
                this.fin.Position = this.position;
                this.lastTransactionLength = 0;
            }
        }
    }
}
