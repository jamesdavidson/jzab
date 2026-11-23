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

using System.Collections.Generic;
using jzab;

using Microsoft.Extensions.Logging;

namespace NZab
{
    /**
     * Abstract transport class. Used for communication between different
     * Zab instances. It will handle connections to different peers underneath.
     * When the transport is disconnected from a destination, it calls
     * onDisconnected() callback, and it stops sending/receiving messages to/from
     * the destination. Any subsequent send() call to the destination will simply be
     * ignored. To tell the transport to reconnect to the destination, the caller
     * needs to call clear() method. The purpose of the clear() method is for the
     * caller to explicitly acknowledge the onDisconnected event.
     */
    internal abstract class Transport
    {
        private static readonly ILogger LOG = LoggerFactory.Create(builder =>
        {
            builder.AddSimpleConsole();
            builder.SetMinimumLevel(LogLevel.Debug);
        }).CreateLogger(typeof(Transport));

        protected Receiver receiver;

        public Transport(Receiver r)
        {
            this.receiver = r;
        }

        /**
         * Sends a message to a specific server. The channel delivers
         * the message in FIFO order. Transport establishes a connection to the
         * destination implicitly the first time this method is called with a given
         * destination.
         *
         * @param destination the id of the message destination
         * @param message the message to be sent
         */
        public abstract void Send(string destination, Message message);

        /**
         * Sends a file to a specific server.
         *
         * @param destination the id of the message destination
         * @param filePath the path of the file to be sent
         */
        public abstract void Send(string destination, string filePath);

        /**
         * Clears the connection to the destination. If there is no connection to the
         * destination, this method does nothing. This method clears any pending
         * outgoing messages. Transport reestablishes the connection on the next
         * send(). Note that Receiver.onDisconnected() callback will not be invoked
         * when the caller clears the connection using this method.
         *
         * @param destination the id of the destination
         */
        public abstract void Clear(string destination);

        /**
         * Broadcasts a message to a set of peers.
         *
         * @param peers the set of destination peers.
         * @param message the message to be broadcasted.
         */
        public void Broadcast(IEnumerable<string> peers, Message message)
        {
            foreach (string peer in peers)
            {
                Send(peer, message);
            }
        }

        /**
         * Shutdown the transport.
         */
        public abstract void Shutdown();

        /**
         * Interface of receiver class. Transport will notify the receiver of
         * arrived messages.
         */
        public interface Receiver
        {
            /**
             * Callback that will be called by Transport class once the message
             * is arrived. The message is guaranteed to be received in FIFO order,
             * which means if message m1 is sent before m2 from sender s, then m1
             * is guaranteed to be received first.
             *
             * @param source the id of the server who sent the message
             * @param message the message
             */
            void OnReceived(string source, Message message);

            /**
             * Callback that notifies that the transport has been disconnected from the
             * destination. Once this callback is invoked, the transport stops sending/
             * receiving messages to/from the destination. You need to call the clear()
             * method to tell the transport it's ok to reconnect.
             *
             * @param destination the ID of the peer from which the transport got
             *                    disconnected
             */
            void OnDisconnected(string destination);
        }
    }
}
