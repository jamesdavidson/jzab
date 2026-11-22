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
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

namespace NZab
{
    /**
     * Persistent variable for Zab.
     */
    internal class PersistentState
    {
        protected ILog log;
        private readonly string fAckEpoch;
        private readonly string fProposedEpoch;
        private readonly string rootDir;
        private string dataDir;
        private bool isTransferring = false;

        public PersistentState(string dir) : this(dir, null) { }

        public PersistentState(string dir, ILog log)
        {
            this.rootDir = dir;
            if (!Directory.Exists(rootDir))
            {
                Directory.CreateDirectory(rootDir);
            }
            this.dataDir = GetLatestDataDir();
            if (this.dataDir == null)
            {
                this.dataDir = GetNextDataDir();
                Directory.CreateDirectory(this.dataDir);
            }
            this.fAckEpoch = Path.Combine(rootDir, "ack_epoch");
            this.fProposedEpoch = Path.Combine(rootDir, "proposed_epoch");
            if (log == null)
            {
                this.log = CreateLog(this.dataDir);
            }
            else
            {
                this.log = log;
            }
        }

        protected virtual ILog CreateLog(string dir)
        {
            return new RollingLog(dir, ZabConfig.ROLLING_SIZE);
        }

        public Zxid GetLatestZxid()
        {
            Zxid zxid = this.log.GetLatestZxid();
            if (zxid.CompareTo(Zxid.ZXID_NOT_EXIST) == 0)
            {
                zxid = GetSnapshotZxid();
            }
            return zxid;
        }

        public long GetAckEpoch()
        {
            try
            {
                return FileUtils.ReadLongFromFile(this.fAckEpoch);
            }
            catch (FileNotFoundException)
            {
                return -1;
            }
        }

        public void SetAckEpoch(long ackEpoch)
        {
            FileUtils.WriteLongToFile(ackEpoch, this.fAckEpoch);
        }

        public long GetProposedEpoch()
        {
            try
            {
                return FileUtils.ReadLongFromFile(this.fProposedEpoch);
            }
            catch (FileNotFoundException)
            {
                return -1;
            }
        }

        public void SetProposedEpoch(long pEpoch)
        {
            FileUtils.WriteLongToFile(pEpoch, this.fProposedEpoch);
            FsyncDirectory();
        }

        public ClusterConfiguration GetLastSeenConfig()
        {
            string file = GetLatestFileWithPrefix(this.rootDir, "cluster_config");
            if (file == null)
            {
                return null;
            }
            try
            {
                NameValueCollection prop = FileUtils.ReadPropertiesFromFile(file);
                return ClusterConfiguration.FromProperties(prop);
            }
            catch (FileNotFoundException)
            {
                return null;
            }
        }

        public ClusterConfiguration GetLastConfigWithin(Zxid zxid)
        {
            string pattern = @"cluster_config\.\d+_-?\d+";
            string zxidFileName = "cluster_config." + zxid.ToSimpleString();
            string lastFileName = null;

            foreach (string filePath in Directory.GetFiles(this.rootDir))
            {
                string fileName = Path.GetFileName(filePath);
                if (Regex.IsMatch(fileName, pattern))
                {
                    if (lastFileName == null && string.Compare(fileName, zxidFileName) <= 0)
                    {
                        lastFileName = fileName;
                    }
                    else if (lastFileName != null &&
                             string.Compare(fileName, lastFileName) > 0 &&
                             string.Compare(fileName, zxidFileName) <= 0)
                    {
                        lastFileName = fileName;
                    }
                }
            }
            if (lastFileName == null)
            {
                return null;
            }
            string targetFile = Path.Combine(this.rootDir, lastFileName);
            try
            {
                NameValueCollection prop = FileUtils.ReadPropertiesFromFile(targetFile);
                return ClusterConfiguration.FromProperties(prop);
            }
            catch (FileNotFoundException)
            {
                return null;
            }
        }

        public void SetLastSeenConfig(ClusterConfiguration conf)
        {
            string version = conf.GetVersion().ToSimpleString();
            string file = Path.Combine(rootDir, string.Format("cluster_config.{0}", version));
            FileUtils.WritePropertiesToFile(conf.ToProperties(), file);
            FsyncDirectory();
        }

        public ILog GetLog()
        {
            return this.log;
        }

        public bool IsEmpty()
        {
            return Directory.GetFiles(this.rootDir).Length == 0 && Directory.GetDirectories(this.rootDir).Length <= 1;
        }

        public string SetSnapshotFile(string tempFile, Zxid zxid)
        {
            string snapshot = Path.Combine(dataDir, string.Format("snapshot.{0}", zxid.ToSimpleString()));
            FileUtils.AtomicMove(tempFile, snapshot);
            FsyncDirectory();
            return snapshot;
        }

        public string GetSnapshotFile()
        {
            return GetLatestFileWithPrefix(this.dataDir, "snapshot");
        }

        public Zxid GetSnapshotZxid()
        {
            string snapshot = GetSnapshotFile();
            if (snapshot == null)
            {
                return Zxid.ZXID_NOT_EXIST;
            }
            string fileName = Path.GetFileName(snapshot);
            string strZxid = fileName.Substring(fileName.IndexOf('.') + 1);
            return Zxid.FromSimpleString(strZxid);
        }

        public string CreateTempFile(string prefix)
        {
            string tempPath = Path.Combine(this.rootDir, prefix + Guid.NewGuid().ToString());
            File.Create(tempPath).Dispose();
            return tempPath;
        }

        public string GetLogDir()
        {
            return this.rootDir;
        }

        public string GetLatestFileWithPrefix(string dir, string prefix)
        {
            List<string> files = GetFilesWithPrefix(dir, prefix);
            if (files.Count > 0)
            {
                return files[files.Count - 1];
            }
            return null;
        }

        public List<string> GetFilesWithPrefix(string dir, string prefix)
        {
            List<string> files = new List<string>();
            string pattern = "^" + Regex.Escape(prefix) + @"\.\d+_-?\d+$";
            foreach (string filePath in Directory.GetFiles(dir))
            {
                string fileName = Path.GetFileName(filePath);
                if (Regex.IsMatch(fileName, pattern))
                {
                    files.Add(filePath);
                }
            }
            if (files.Count > 0)
            {
                files.Sort((a, b) => string.Compare(Path.GetFileName(a), Path.GetFileName(b)));
            }
            return files;
        }

        void FsyncDirectory()
        {
            // C# doesn't have a direct equivalent to fsyncing a directory like Java's FileChannel.force(true) on a directory path.
            // On Windows, this is generally not required/supported in the same way.
        }

        public void BeginStateTransfer()
        {
            this.isTransferring = true;
            string tempDirName = "tmp_data" + Guid.NewGuid().ToString();
            this.dataDir = Path.Combine(this.rootDir, tempDirName);
            Directory.CreateDirectory(this.dataDir);
            this.log = CreateLog(this.dataDir);
        }

        public void EndStateTransfer()
        {
            string nextDir = GetNextDataDir();
            FileUtils.AtomicMove(this.dataDir, nextDir);
            this.dataDir = nextDir;
            this.log = CreateLog(dataDir);
            FsyncDirectory();
            this.isTransferring = false;
        }

        public bool IsInStateTransfer()
        {
            return this.isTransferring;
        }

        public void UndoStateTransfer()
        {
            this.isTransferring = false;
            this.dataDir = GetLatestDataDir();
            this.log = CreateLog(dataDir);
        }

        public string GetLatestDataDir()
        {
            List<string> dirs = new List<string>();
            string pattern = @"^data\d+$";
            foreach (string dirPath in Directory.GetDirectories(this.rootDir))
            {
                string dirName = Path.GetFileName(dirPath);
                if (Regex.IsMatch(dirName, pattern))
                {
                    dirs.Add(dirPath);
                }
            }
            if (dirs.Count > 0)
            {
                dirs.Sort((a, b) => string.Compare(Path.GetFileName(a), Path.GetFileName(b)));
                return dirs[dirs.Count - 1];
            }
            return null;
        }

        public string GetNextDataDir()
        {
            string latest = GetLatestDataDir();
            long newID;
            if (latest == null)
            {
                newID = 0;
            }
            else
            {
                string latestName = Path.GetFileName(latest);
                newID = long.Parse(latestName.Substring(4)) + 1;
            }
            string suffix = newID.ToString("D15");
            return Path.Combine(this.rootDir, "data" + suffix);
        }

        public void CleanupClusterConfigFiles()
        {
            Zxid latestZxid = GetLatestZxid();
            List<string> files = GetFilesWithPrefix(this.rootDir, "cluster_config");
            if (files.Count == 0)
            {
                throw new Exception("There's no cluster_config files!");
            }
            foreach (string file in files)
            {
                string fileName = Path.GetFileName(file);
                string strZxid = fileName.Substring(fileName.IndexOf('.') + 1);
                Zxid zxid = Zxid.FromSimpleString(strZxid);
                if (zxid.CompareTo(latestZxid) > 0)
                {
                    File.Delete(file);
                }
            }
            FsyncDirectory();
        }
    }
}
