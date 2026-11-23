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
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

using Microsoft.Extensions.Logging;

namespace NZab
{
    /**
     * Rolling log implementation.
     */
    internal class RollingLog : ILog
    {
        private static readonly ILogger LOG = LoggerFactory.Create(builder =>
        {
            builder.AddSimpleConsole();
            builder.SetMinimumLevel(LogLevel.Debug);
        }).CreateLogger(typeof(RollingLog));

        private readonly long rollingSize;
        private readonly List<string> logFiles = new List<string>();
        private SimpleLog currentLog;
        private readonly string logDir;
        private Zxid lastSeenZxid = Zxid.ZXID_NOT_EXIST;

        public RollingLog(string logDir, long rollingSize)
        {
            this.logDir = logDir;
            this.rollingSize = rollingSize;
            InitFromDir();
            this.currentLog = GetLastLog();
            this.lastSeenZxid = GetLatestZxid();
        }

        public void Dispose()
        {
            if (this.currentLog != null)
            {
                this.currentLog.Dispose();
                this.currentLog = null;
            }
        }

        public void Append(Transaction txn)
        {
            if (this.lastSeenZxid.CompareTo(txn.GetZxid()) >= 0)
            {
                throw new Exception("The zxid is not larger than last seen zxid in the log.");
            }
            if (currentLog == null || new FileInfo(Path.Combine(logDir, Path.GetFileName(logFiles.Last()))).Length >= this.rollingSize)
            {
                Zxid zxid = txn.GetZxid();
                this.Dispose();
                string logFileName = "transaction." + zxid.ToSimpleString();
                string logFilePath = Path.Combine(logDir, logFileName);
                this.logFiles.Add(logFilePath);
                this.currentLog = new SimpleLog(logFilePath);
            }
            this.lastSeenZxid = txn.GetZxid();
            this.currentLog.Append(txn);
        }

        public void Truncate(Zxid zxid)
        {
            int lastKeepIdx = GetFileIdx(zxid);
            for (int i = lastKeepIdx + 1; i < logFiles.Count; ++i)
            {
                File.Delete(logFiles[i]);
            }
            if (lastKeepIdx != -1)
            {
                using (SimpleLog log = new SimpleLog(this.logFiles[lastKeepIdx]))
                {
                    log.Truncate(zxid);
                }
            }
            if (lastKeepIdx + 1 < logFiles.Count)
            {
                logFiles.RemoveRange(lastKeepIdx + 1, logFiles.Count - (lastKeepIdx + 1));
            }
            this.currentLog = GetLastLog();
            this.lastSeenZxid = GetLatestZxid();
        }

        public Zxid GetLatestZxid()
        {
            if (logFiles.Count == 0) return Zxid.ZXID_NOT_EXIST;
            using (SimpleLog log = GetLastLog())
            {
                return log.GetLatestZxid();
            }
        }

        public ILogIterator GetIterator(Zxid zxid)
        {
            return new RollingLogIterator(this, zxid);
        }

        public DivergingTuple FirstDivergingPoint(Zxid zxid)
        {
            int idx = GetFileIdx(zxid);
            if (idx == -1)
            {
                return new DivergingTuple(new RollingLogIterator(this, Zxid.ZXID_NOT_EXIST), Zxid.ZXID_NOT_EXIST);
            }
            Zxid firstZxid = GetZxidFromFileName(logFiles[idx]);
            var iter = new RollingLogIterator(this, firstZxid);
            Zxid prevZxid = firstZxid;
            while (iter.HasNext())
            {
                Zxid curZxid = iter.Next().GetZxid();
                if (curZxid.CompareTo(zxid) == 0) return new DivergingTuple(iter, zxid);
                if (curZxid.CompareTo(zxid) > 0)
                {
                    iter.Dispose();
                    return new DivergingTuple(new RollingLogIterator(this, curZxid), prevZxid);
                }
                prevZxid = curZxid;
            }
            return new DivergingTuple(iter, prevZxid);
        }

        public void Sync()
        {
            if (this.currentLog != null) this.currentLog.Sync();
        }

        public void Trim(Zxid zxid)
        {
            throw new NotSupportedException();
        }

        private void InitFromDir()
        {
            string pattern = @"^transaction\.\d+_\d+$";
            foreach (string filePath in Directory.GetFiles(this.logDir))
            {
                if (Regex.IsMatch(Path.GetFileName(filePath), pattern))
                {
                    this.logFiles.Add(filePath);
                }
            }
            if (this.logFiles.Count > 0)
            {
                this.logFiles.Sort((a, b) => string.Compare(Path.GetFileName(a), Path.GetFileName(b)));
            }
        }

        private int GetFileIdx(Zxid zxid)
        {
            if (logFiles.Count == 0 || zxid.CompareTo(GetZxidFromFileName(logFiles[0])) < 0)
            {
                return -1;
            }
            int idx = 0;
            while (idx < logFiles.Count - 1)
            {
                Zxid firstZxid = GetZxidFromFileName(logFiles[idx]);
                if (zxid.CompareTo(firstZxid) == 0) break;
                else if (zxid.CompareTo(firstZxid) > 0)
                {
                    int nextIdx = idx + 1;
                    Zxid nextFirstZxid = GetZxidFromFileName(logFiles[nextIdx]);
                    if (zxid.CompareTo(nextFirstZxid) < 0) break;
                }
                idx++;
            }
            return idx;
        }

        private Zxid GetZxidFromFileName(string filePath)
        {
            string fileName = Path.GetFileName(filePath);
            string strZxid = fileName.Substring(fileName.IndexOf('.') + 1);
            return Zxid.FromSimpleString(strZxid);
        }

        private SimpleLog GetLastLog()
        {
            if (logFiles.Count == 0) return null;
            return new SimpleLog(logFiles.Last());
        }

        class RollingLogIterator : ILogIterator
        {
            private readonly RollingLog outer;
            private int fileIdx;
            private ILogIterator iter;

            public RollingLogIterator(RollingLog outer, Zxid startZxid)
            {
                this.outer = outer;
                int idx = outer.GetFileIdx(startZxid);
                if (outer.logFiles.Count == 0)
                {
                    this.fileIdx = -1;
                    this.iter = null;
                }
                else
                {
                    if (idx == -1) idx = 0;
                    this.fileIdx = idx;
                    using (SimpleLog log = new SimpleLog(outer.logFiles[this.fileIdx]))
                    {
                        this.iter = log.GetIterator(startZxid);
                    }
                }
            }

            public void Dispose()
            {
                if (this.iter != null) this.iter.Dispose();
            }

            public bool HasNext()
            {
                return this.fileIdx != -1 && (this.iter.HasNext() || this.fileIdx < (outer.logFiles.Count - 1));
            }

            public Transaction Next()
            {
                if (!HasNext()) throw new Exception("No more elements");
                if (!this.iter.HasNext())
                {
                    this.iter.Dispose();
                    this.fileIdx++;
                    this.iter = new SimpleLog.SimpleLogIterator(outer.logFiles[this.fileIdx]);
                }
                return this.iter.Next();
            }
        }
    }
}
