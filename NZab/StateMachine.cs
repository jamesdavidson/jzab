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
using System.IO;

namespace NZab
{
    /**
     * The state machine interface for interacting with Zab.
     */
    public interface StateMachine
    {
        /**
         * Converts a message to an idempotent transaction. This method is called only
         * on the leader after the Zxid has been assigned but before proposing the
         * message to followers.
         *
         * @param zxid zxid of the message.
         * @param message the original message
         * @return an idempotent state update based on the original message. This is
         * what gets proposed to followers.
         */
        byte[] Preprocess(Zxid zxid, byte[] message);

        /**
         * Callback for Zab.Send. This method is called from a single
         * thread to ensure that the state updates are applied in the same order
         * as they arrived. Zab assumes that the transaction is successfully
         * applied to the state machine when this method returns.
         *
         * @param zxid zxid of the message
         * @param stateUpdate the incremental state update
         * @param clientId the id of the client who sends the request. The request
         * delivered in RECOVERING phase has clientId sets to null.
         * @param ctx the context object.
         */
        void Deliver(Zxid zxid, byte[] stateUpdate, string clientId, object ctx);

        /**
         * Callback for Zab.Flush. This method is called from the same
         * thread as the Deliver callback.
         *
         * @param zxid zxid of the flush request.
         * @param flushRequest the flush request.
         * @param ctx the context object.
         */
        void Flushed(Zxid zxid, byte[] flushRequest, object ctx);

        /**
         * Callback for Zab.TakeSnapshot.
         *
         * @param filePath the path for the snapshot file.
         * @param ctx the context object.
         */
        void SnapshotDone(string filePath, object ctx);

        /**
         * Callback for Zab.Remove.
         *
         * @param serverId the ID of the server whom gets removed.
         * @param ctx the context object.
         */
        void Removed(string serverId, object ctx);

        /**
         * Serializes the application state to a FileStream.
         *
         * @param fs file to write the state to.
         */
        void Save(System.IO.Stream fs);

        /**
         * Deserializes the application state from a FileStream.
         *
         * @param fs file to read the state from.
         */
        void Restore(System.IO.Stream fs);

        /**
         * Notifies the state machine that Zab is in recovering phase. When
         * Zab goes from leading/following phase to recovering phase, there
         * might be requests that have been submitted to Zab but haven't been
         * delivered back to the state machine. These requests, along with their
         * associated contexts, are passed backed to the state machine. Note that
         * these requests might or might not have been committed by Zab.
         *
         * @param pendingRequests the pending requests.
         */
        void Recovering(PendingRequests pendingRequests);

        /**
         * Notifies the state machine that Zab is in leading phase with given
         * sets of active followers and cluster members. This method gets called when
         * Zab transitions from recovering phase to leading phase. It also
         * gets called when there is a change in active followers or cluster
         * membership while Zab is in leading phase. This method is called
         * from the same thread as Deliver.
         *
         * @param activeFollowers current alive followers.
         * @param clusterMembers the members of new configuration.
         */
        void Leading(ISet<string> activeFollowers, ISet<string> clusterMembers);

        /**
         * Notifies the state machine that Zab is in following phase with
         * given leader and cluster members. This method gets called when Zab
         * transitions from recovering phase to following phase. It also gets called
         * when there is a change in cluster membership while Zab is in
         * following phase. This method is called from the same thread as
         * Deliver.
         *
         * @param leader current leader.
         * @param clusterMembers the members of new configuration.
         */
        void Following(string leader, ISet<string> clusterMembers);
    }
}
