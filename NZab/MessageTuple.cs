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

namespace NZab
{
    /**
     * Class that holds both message and source/destination.
     */
    internal class MessageTuple
    {
        private readonly string serverId;
        private readonly Message message;
        private Zxid zxid;

        public static readonly MessageTuple REQUEST_OF_DEATH =
            new MessageTuple(null, null);

        public MessageTuple(string serverId, Message message)
            : this(serverId, message, null)
        {
        }

        public MessageTuple(string serverId, Message message, Zxid zxid)
        {
            this.serverId = serverId;
            this.message = message;
            this.zxid = zxid;
        }

        public string GetServerId()
        {
            return this.serverId;
        }

        public Message GetMessage()
        {
            return this.message;
        }

        public void SetZxid(Zxid z)
        {
            this.zxid = z;
        }

        public Zxid GetZxid()
        {
            return this.zxid;
        }
    }
}
