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

namespace NZab
{
    /**
     * Stores different kinds of pending requests.
     */
    public class PendingRequests
    {
        internal PendingRequests() { }

        /**
         * The pending send requests.
         *
         * The first element of the tuple is the request sent, the second element is
         * the corresponding context
         */
        public readonly List<Tuple> PendingSends = new List<Tuple>();

        /**
         * The pending flush requests.
         * The first element of the tuple is flush request, the second element is ctx.
         */
        public readonly List<Tuple> PendingFlushes = new List<Tuple>();

        /**
         * The pending remove requests.
         * The first element of tuple is serverId, the second element is ctx.
         */
        public readonly List<Tuple> PendingRemoves = new List<Tuple>();

        /**
         * The pending snapshot requests.
         */
        public readonly List<object> PendingSnapshots = new List<object>();

        /**
         * The tuple holds both request and ctx.
         */
        public class Tuple
        {
            public readonly object Param;
            public readonly object Ctx;

            internal Tuple(object param, object ctx)
            {
                this.Param = param;
                this.Ctx = ctx;
            }
        }
    }
}
