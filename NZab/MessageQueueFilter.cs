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

namespace NZab
{
    /**
     * MessageQueueFilter is a filter class.
     */
    internal class MessageQueueFilter
    {
        private readonly BlockingCollection<MessageTuple> messageQueue;

        public MessageQueueFilter(BlockingCollection<MessageTuple> messageQueue)
        {
            this.messageQueue = messageQueue;
        }

        public virtual MessageTuple GetMessage(int timeoutMs)
        {
            MessageTuple tuple;
            if (!messageQueue.TryTake(out tuple, timeoutMs))
            {
                throw new TimeoutException("Timeout while waiting for the message.");
            }
            if (tuple.GetMessage().type == Message.MessageType.SHUT_DOWN)
            {
                throw new Participant.LeftCluster("Left cluster");
            }
            return tuple;
        }

        public virtual MessageTuple GetExpectedMessage(Message.MessageType type, string source, int timeoutMs)
        {
            long startTime = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
            while (true)
            {
                long now = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
                int remainMs = timeoutMs - (int)(now - startTime);
                if (remainMs < 0) remainMs = 0;

                MessageTuple tuple = GetMessage(remainMs);
                string from = tuple.GetServerId();
                if (tuple.GetMessage().type == type && (source == null || source.Equals(from)))
                {
                    return tuple;
                }
                else
                {
                    if (DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond - startTime >= timeoutMs)
                    {
                        throw new TimeoutException("Timeout in GetExpectedMessage.");
                    }
                }
            }
        }
    }
}
