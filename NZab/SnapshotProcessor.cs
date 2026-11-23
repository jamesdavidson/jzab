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
using System.IO;
using System.Threading.Tasks;
using jzab;
using Microsoft.Extensions.Logging;

namespace NZab
{
    /**
     * The processor which is responsible for taking the snapshot of application
     * when it's required.
     */
    internal class SnapshotProcessor : RequestProcessor
    {
        private static readonly ILogger LOG = LoggerFactory.Create(b =>
        {
            b.AddSimpleConsole();
            b.SetMinimumLevel(LogLevel.Debug);
        }).CreateLogger(typeof(SnapshotProcessor));

        private readonly BlockingCollection<MessageTuple> requestQueue = new BlockingCollection<MessageTuple>();
        private readonly StateMachine stateMachine;
        private readonly PersistentState persistence;
        private readonly Transport transport;
        private readonly string serverId;
        private readonly Task task;

        public SnapshotProcessor(StateMachine stateMachine,
                                 PersistentState persistence,
                                 string serverId,
                                 Transport transport)
        {
            this.stateMachine = stateMachine;
            this.persistence = persistence;
            this.serverId = serverId;
            this.transport = transport;
            this.task = Task.Factory.StartNew(Run, TaskCreationOptions.LongRunning);
        }

        public void ProcessRequest(MessageTuple request)
        {
            this.requestQueue.Add(request);
        }

        public void Shutdown()
        {
            this.requestQueue.Add(MessageTuple.REQUEST_OF_DEATH);
            this.task.Wait();
        }

        private void Run()
        {
            try
            {
                while (true)
                {
                    MessageTuple request = requestQueue.Take();
                    if (request == MessageTuple.REQUEST_OF_DEATH) break;

                    Message msg = request.GetMessage();
                    if (msg.type == Message.MessageType.SNAPSHOT)
                    {
                        Zxid zxid = MessageBuilder.FromProtoZxid(msg.snapshot.last_zxid);
                        string temp = persistence.CreateTempFile("snapshot");
                        using (FileStream fout = new FileStream(temp, FileMode.Open, FileAccess.Write))
                        {
                            stateMachine.Save(fout);
                            fout.Flush();
                        }
                        string file = persistence.SetSnapshotFile(temp, zxid);
                        Message done = MessageBuilder.BuildSnapshotDone(file);
                        this.transport.Send(this.serverId, done);
                    }
                }
            }
            catch (Exception) { }
        }
    }
}
