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
using Microsoft.Extensions.Logging;

namespace NZab
{
    /**
     * Interface of manipulating transaction log.
     */
    internal interface ILog : IDisposable
    {
        /**
         * Appends a request to transaction log.
         *
         * @param txn the transaction which will be appended to log.
         */
        void Append(Transaction txn);

        /**
         * Truncates this transaction log at the given zxid.
         *
         * This method deletes all the transactions with zxids
         * higher than the given zxid.
         *
         * @param zxid the transaction id.
         */
        void Truncate(Zxid zxid);

        /**
         * Gets the latest transaction id from log.
         *
         * @return the transaction id of the latest transaction.
         * Return Zxid.ZXID_NOT_EXIST if the log is empty.
         */
        Zxid GetLatestZxid();

        /**
         * Gets an iterator to read transactions from this log starting
         * at the given zxid (including zxid).
         *
         * @param zxid the id of the transaction.
         * @return an iterator to read the next transaction in logs.
         */
        ILogIterator GetIterator(Zxid zxid);

        /**
         * Given a zxid, finds out the first diverging point and returns a tuple
         * of (diverging zxid, iterator points to next txn after diverging zxid).
         * If there's no diverging point, which means the zxid is a prefix of the
         * server's log, then the diverging zxid will be as same as giving zxid.
         *
         * @param zxid the id of the transaction.
         * @return a tuple holds first diverging zxid and an iterator points to
         * subsequent transactions.
         */
        DivergingTuple FirstDivergingPoint(Zxid zxid);

        /**
         * Syncs all the appended transactions to the physical media.
         */
        void Sync();

        /**
         * Trim the log up to the transaction of zxid inclusively.
         *
         * @param zxid the last zxid(inclusive) which will be trimmed to.
         */
        void Trim(Zxid zxid);
    }

    /**
     * Interface of iterating the transaction log.
     */
    internal interface ILogIterator : IDisposable
    {
        /**
         * Checks if it has next transaction record.
         *
         * @return true if it has more transactions, false otherwise.
         */
        bool HasNext();

        /**
         * Goes to the next transaction.
         *
         * @return the next transaction record
         */
        Transaction Next();
    }

    /**
     * The tuple holds the return value of FirstDivergingPoint function.
     */
    internal class DivergingTuple
    {
        private static readonly ILogger LOG = LoggerFactory.Create(builder =>
        {
            builder.AddSimpleConsole();
            builder.SetMinimumLevel(LogLevel.Debug);
        }).CreateLogger(typeof(DivergingTuple));

        private readonly ILogIterator iter;
        private readonly Zxid zxid;

        public DivergingTuple(ILogIterator iter, Zxid zxid)
        {
            this.iter = iter;
            this.zxid = zxid;
        }

        public ILogIterator GetIterator()
        {
            return this.iter;
        }

        public Zxid GetDivergingZxid()
        {
            return this.zxid;
        }
    }
}
