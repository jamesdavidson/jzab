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

using System.IO;

namespace NZab
{
    /**
     * Configuration parameters for Zab.
     */
    public class ZabConfig
    {
        /**
         * Maximum number of pending requests allowed for each server.
         */
        internal const int MAX_PENDING_REQS = 5000;
        internal const long ROLLING_SIZE = 1024 * 1024 * 1024;
        private int timeoutMs = 1000;
        private int minSyncTimeoutMs = 3000;
        // The default logDir is current working directory.
        private string logDir = Directory.GetCurrentDirectory();
        private int maxBatchSize = 1000;
        private SslParameters sslParam = new SslParameters();

        /**
         * Gets the directory for storing transaction log.
         *
         * @return the directory for storing transaction log, if it's undefined,
         * return the current working directory.
         */
        public string GetLogDir()
        {
            return this.logDir;
        }

        /**
         * Sets the directory for the persistent states.
         *
         * @param dir the log directory path.
         */
        public void SetLogDir(string dir)
        {
            this.logDir = dir;
        }

        /**
         * Gets the timeout of heartbeat messages (default is 1000 milliseconds).
         *
         * @return the timeout in milliseconds
         */
        public int GetTimeoutMs()
        {
            return this.timeoutMs;
        }

        /**
         * Sets the timeout of heartbeat message.
         *
         * @param timeout the timeout is milliseconds.
         */
        public void SetTimeoutMs(int timeout)
        {
            this.timeoutMs = timeout;
        }

        /**
         * Gets the timeout for synchronizing peers (default is 3000 milliseconds).
         *
         * @return the timeout in milliseconds.
         */
        public int GetMinSyncTimeoutMs()
        {
            return this.minSyncTimeoutMs;
        }

        /**
         * Sets the minimum timeout.
         *
         * @param timeout the timeout in milliseconds.
         */
        public void SetMinSyncTimeoutMs(int timeout)
        {
            this.minSyncTimeoutMs = timeout;
        }

        /**
         * Gets the maximum batch size of SyncProposalProcessor. SyncProposalProcessor
         * will try to batch several transactions with one fsync and acknowledgement
         * to improve throughput. Its default value is 1000 transactions.
         *
         * @return the maximum batch size for SyncProposalProcessor.
         */
        public int GetMaxBatchSize()
        {
            return this.maxBatchSize;
        }

        /**
         * Sets the maximum batching size for SyncProposalProcessor.
         *
         * @param batchSize the maximum batching size.
         */
        public void SetMaxBatchSize(int batchSize)
        {
            this.maxBatchSize = batchSize;
        }

        /**
         * Sets the SSL parameters for Jzab.
         *
         * @param param the SSL parameters.
         */
        public void SetSslParameters(SslParameters param)
        {
            this.sslParam = param;
        }

        /**
         * Gets the SSL parameters for Jzab.
         *
         * @return the SSL parameters.
         */
        public SslParameters GetSslParameters()
        {
            return this.sslParam;
        }

        /**
         * SSL-related parameters.
         */
        public class SslParameters
        {
            private readonly string keyStore;
            private readonly string keyStorePassword;
            private readonly string trustStore;
            private readonly string trustStorePassword;

            /**
             * @param keyStore keystore file that contains the private key and
             *                 corresponding certificate chain.
             * @param keyStorePassword password for the keystore, or null if the
             *                         password is not set.
             * @param trustStore truststore file that contains trusted CA certificates.
             * @param trustStorePassword password for the truststore, or null if the
             *                           password is not set.
             */
            public SslParameters(string keyStore, string keyStorePassword,
                                 string trustStore, string trustStorePassword)
            {
                this.keyStore = keyStore;
                this.keyStorePassword = keyStorePassword;
                this.trustStore = trustStore;
                this.trustStorePassword = trustStorePassword;
            }

            public SslParameters()
            {
                this.keyStore = null;
                this.keyStorePassword = null;
                this.trustStore = null;
                this.trustStorePassword = null;
            }

            public string GetKeyStore()
            {
                return this.keyStore;
            }

            public string GetTrustStore()
            {
                return this.trustStore;
            }

            public string GetKeyStorePassword()
            {
                return this.keyStorePassword;
            }

            public string GetTrustStorePassword()
            {
                return this.trustStorePassword;
            }
        }
    }
}
