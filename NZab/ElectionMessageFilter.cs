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
using jzab;
using Microsoft.Extensions.Logging;

namespace NZab
{
    /**
     * ElectionMessageFilter is a filter class that handles election messages.
     */
    internal class ElectionMessageFilter : MessageQueueFilter
    {
        private static readonly ILogger LOG = LoggerFactory.Create(b =>
        {
            b.AddSimpleConsole();
            b.SetMinimumLevel(LogLevel.Debug);
        }).CreateLogger(typeof(ElectionMessageFilter));

        protected readonly Election election;

        public ElectionMessageFilter(BlockingCollection<MessageTuple> messageQueue, Election election)
            : base(messageQueue)
        {
            this.election = election;
        }

        public override MessageTuple GetMessage(int timeoutMs)
        {
            long startTime = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
            while (true)
            {
                long now = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
                int remainMs = timeoutMs - (int)(now - startTime);
                if (remainMs < 0) remainMs = 0;

                MessageTuple tuple = base.GetMessage(remainMs);
                Message msg = tuple.GetMessage();

                if (msg.type == Message.MessageType.ELECTION_INFO)
                {
                    election.Reply(tuple);
                    if (DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond - startTime >= timeoutMs)
                    {
                        throw new TimeoutException("Timeout while waiting for the message.");
                    }
                    continue;
                }
                return tuple;
            }
        }
    }
}
