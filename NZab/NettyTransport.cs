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
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using DotNetty.Buffers;
using DotNetty.Codecs;
using DotNetty.Handlers.Streams;
using DotNetty.Handlers.Tls;
using DotNetty.Transport.Bootstrapping;
using DotNetty.Transport.Channels;
using DotNetty.Transport.Channels.Sockets;
using DotNetty.Common.Utilities;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using Structmap.NZab;

namespace NZab
{
    /**
     * Netty-based transport.
     */
    internal class NettyTransport : Transport
    {
        private static readonly ILogger LOG = LoggerFactory.Create(builder => builder.AddConsole()).CreateLogger<NettyTransport>();
        static readonly AttributeKey<string> SENDER = AttributeKey<string>.ValueOf("remote");

        private readonly string hostPort;
        private readonly IEventLoopGroup bossGroup = new MultithreadEventLoopGroup(1);
        private readonly IEventLoopGroup workerGroup = new MultithreadEventLoopGroup();
        IChannel channel;
        private readonly string keyStore;
        private readonly string keyStorePassword;
        private readonly string trustStore;
        private readonly string trustStorePassword;
        private X509Certificate2 serverCertificate;
        private X509Certificate2 clientCertificate;
        private readonly string dir;

        // remote id => sender map.
        ConcurrentDictionary<string, Sender> senders = new ConcurrentDictionary<string, Sender>();

        // remote id => incoming channel context
        ConcurrentDictionary<string, IChannelHandlerContext> receivers =
            new ConcurrentDictionary<string, IChannelHandlerContext>();

        public NettyTransport(string hostPort, Receiver receiver, string dir)
            : this(hostPort, receiver, new ZabConfig.SslParameters(), dir)
        {
        }

        /**
         * Constructs a NettyTransport object.
         *
         * @param hostPort "hostname:port" string. The netty transport binds to the
         *                 port specified in the string.
         * @param receiver receiver callback.
         * @param sslParam Ssl parameters.
         * @param dir the directory used to store the received file.
         */
        public NettyTransport(string hostPort, Receiver receiver,
                              ZabConfig.SslParameters sslParam,
                              string dir)
            : base(receiver)
        {
            this.keyStore = sslParam.GetKeyStore();
            this.trustStore = sslParam.GetTrustStore();
            this.keyStorePassword = sslParam.GetKeyStorePassword();
            this.trustStorePassword = sslParam.GetTrustStorePassword();
            this.dir = dir;
            if (IsSslEnabled())
            {
                InitSsl();
            }

            this.hostPort = hostPort;
            string[] address = hostPort.Split(':', 2);
            int port = int.Parse(address[1]);
            ServerBootstrap b = new ServerBootstrap();
            b.Group(bossGroup, workerGroup)
              .Channel<TcpServerSocketChannel>()
              .Option(ChannelOption.SoBacklog, 128)
              .Option(ChannelOption.SoReuseaddr, true)
              .ChildOption(ChannelOption.SoKeepalive, true)
              .ChildOption(ChannelOption.TcpNodelay, true)
              .ChildHandler(new ActionChannelInitializer<ISocketChannel>(ch =>
              {
                  if (IsSslEnabled())
                  {
                      ch.Pipeline.AddLast(new TlsHandler(serverCertificate, new TlsSettings(TlsRequirement.All, true, null, null)));
                  }
                  // Incoming handlers
                  ch.Pipeline.AddLast(new MainHandler(this));
                  ch.Pipeline.AddLast(new ServerHandshakeHandler(this));
                  ch.Pipeline.AddLast(new NotifyHandler(this));
                  ch.Pipeline.AddLast(new ServerErrorHandler());
              }));

            // Travis build fails once in a while because it fails to bind to a port.
            // This is most likely a transient failure. Retry binding for 5 times with
            // 1 second sleep in between before giving up.
            int bindRetryCount = 5;
            for (int i = 0; ; i++)
            {
                try
                {
                    channel = b.BindAsync(port).Result;
                    LOG.LogInformation("Server started: {0}", hostPort);
                    return;
                }
                catch (Exception ex)
                {
                    if (i >= bindRetryCount)
                    {
                        throw ex;
                    }
                    LOG.LogDebug("Failed to bind to {0}. Retrying after 1 second.", hostPort);
                    Thread.Sleep(1000);
                }
            }
        }

        private bool IsSslEnabled()
        {
            return keyStore != null && trustStore != null;
        }

        private void InitSsl()
        {
            // In .NET, X509Certificate2 is used. For simplicity, we assume the keystore is a PFX/PKCS12 file.
            serverCertificate = new X509Certificate2(keyStore, keyStorePassword);
            clientCertificate = new X509Certificate2(trustStore, trustStorePassword);
        }

        /**
         * Destroys the transport.
         */
        public override void Shutdown()
        {
            try
            {
                channel.CloseAsync().Wait();
                foreach (var entry in senders)
                {
                    entry.Value.Shutdown();
                }
                senders.Clear();

                foreach (var entry in receivers)
                {
                    entry.Value.CloseAsync().Wait();
                }
                receivers.Clear();

            }
            finally
            {
                try
                {
                    TimeSpan quietPeriod = TimeSpan.FromSeconds(0);
                    TimeSpan timeout = TimeSpan.FromSeconds(10);
                    Task wf = workerGroup.ShutdownGracefullyAsync(quietPeriod, timeout);
                    Task bf = bossGroup.ShutdownGracefullyAsync(quietPeriod, timeout);
                    Task.WaitAll(wf, bf);
                    LOG.LogDebug("Shutdown complete: {0}", hostPort);
                }
                catch (Exception ex)
                {
                    LOG.LogDebug(ex, "Interrupted while shutting down NioEventLoopGroup");
                }
            }
        }

        /**
         * Handles server-side handshake.
         */
        private class ServerHandshakeHandler : SimpleChannelInboundHandler<Message>
        {
            private readonly NettyTransport transport;

            public ServerHandshakeHandler(NettyTransport transport)
            {
                this.transport = transport;
            }

            protected override void ChannelRead0(IChannelHandlerContext ctx, Message msg)
            {
                // Make sure it's a handshake message.
                if (msg.Type != Message.Types.MessageType.Handshake)
                {
                    LOG.LogDebug("The first message from {0} was not a handshake",
                              ctx.Channel.RemoteAddress);
                    ctx.CloseAsync();
                    return;
                }
                string remoteId = msg.Handshake.NodeId;

                if (!transport.receivers.TryAdd(remoteId, ctx))
                {
                    LOG.LogWarning("Rejecting a duplicate connection from {0}", remoteId);
                    ctx.CloseAsync();
                    return;
                }
                LOG.LogDebug("{0} accepted a connection from {1}", transport.hostPort, remoteId);

                // Attach the Sender to this channel. Subsequent handlers use this
                // information to determine origins of messages.
                ctx.Channel.GetAttribute(SENDER).Set(remoteId);
                ctx.Pipeline.Remove(this);
            }
        }

        private class MainHandler : ByteToMessageDecoder
        {
            private readonly NettyTransport transport;
            private FileReceiver fileReceiver = null;

            public MainHandler(NettyTransport transport)
            {
                this.transport = transport;
            }

            private Message DecodeToMessage(IByteBuffer @in)
            {
                if (@in.ReadableBytes < 4)
                {
                    return null;
                }
                @in.MarkReaderIndex();
                int messageLength = @in.ReadInt();
                if (@in.ReadableBytes < messageLength)
                {
                    @in.ResetReaderIndex();
                    return null;
                }
                byte[] buffer = new byte[messageLength];
                @in.ReadBytes(buffer);
                try
                {
                    Message msg = Message.Parser.ParseFrom(buffer);
                    return msg;
                }
                catch (InvalidProtocolBufferException e)
                {
                    LOG.LogError(e, "Exception when parse protocol buffer.");
                    // In C# ZabMessage, we don't have MessageBuilder easily available or similar InvalidMessage
                    // So we might need to handle this differently or just throw.
                    // For now, let's just return null or throw.
                    return null;
                }
            }

            protected override void Decode(IChannelHandlerContext context, IByteBuffer @in, List<object> output)
            {
                if (fileReceiver == null)
                {
                    Message msg = DecodeToMessage(@in);
                    if (msg == null)
                    {
                        return;
                    }
                    else if (msg.Type == Message.Types.MessageType.FileHeader)
                    {
                        LOG.LogDebug("Got FILE_HEADER.");
                        fileReceiver = new FileReceiver(msg.FileHeader.Length, transport.dir);
                    }
                    else
                    {
                        output.Add(msg);
                    }
                }
                else
                {
                    fileReceiver.Process(@in);
                    if (fileReceiver.IsDone())
                    {
                        string filePath = fileReceiver.FilePath;
                        Message msg = new Message
                        {
                            Type = Message.Types.MessageType.FileReceived,
                            FileReceived = new FileReceived { FullPath = filePath }
                        };
                        output.Add(msg);
                        // Resets it to null to switch back to normal decode mode.
                        fileReceiver = null;
                    }
                }
            }

            class FileReceiver
            {
                readonly long fileLength;
                long receivedLength = 0;
                readonly string filePath;
                readonly FileStream fout;

                public string FilePath => filePath;

                public FileReceiver(long length, string dir)
                {
                    this.filePath = Path.Combine(dir, "transport" + Guid.NewGuid().ToString());
                    this.fileLength = length;
                    this.fout = new FileStream(this.filePath, FileMode.Create, FileAccess.Write);
                }

                public void Process(IByteBuffer @in)
                {
                    int readableBytes = @in.ReadableBytes;
                    long remainingBytes = fileLength - receivedLength;
                    int bytesToRead = (int)Math.Min(remainingBytes, (long)readableBytes);
                    byte[] buffer = new byte[bytesToRead];
                    @in.ReadBytes(buffer);
                    fout.Write(buffer, 0, bytesToRead);
                    receivedLength += bytesToRead;
                    if (receivedLength == fileLength)
                    {
                        fout.Flush(true);
                        fout.Close();
                    }
                }

                public bool IsDone()
                {
                    return receivedLength == fileLength;
                }
            }
        }

        private class NotifyHandler : SimpleChannelInboundHandler<Message>
        {
            private readonly NettyTransport transport;

            public NotifyHandler(NettyTransport transport)
            {
                this.transport = transport;
            }

            protected override void ChannelRead0(IChannelHandlerContext ctx, Message msg)
            {
                string remoteId = ctx.Channel.GetAttribute(SENDER).Get();
                transport.receiver.OnReceived(remoteId, msg);
            }
        }

        /**
         * Handles server-side errors.
         */
        private class ServerErrorHandler : ChannelHandlerAdapter
        {
            public override void ChannelInactive(IChannelHandlerContext ctx)
            {
                string remote = ctx.Channel.GetAttribute(SENDER).Get();
                if (remote != null)
                {
                    // remote might be null, for example if client connection got closed
                    // because SSL handshake failed.
                    LOG.LogDebug("Disconnected from client {0}", remote);
                }
            }

            public override void ExceptionCaught(IChannelHandlerContext ctx, Exception cause)
            {
                // Don't handle errors here. Call ctx.CloseAsync() and let ChannelInactive()
                // handle all the errors.
                LOG.LogDebug(cause, "Caught an exception");
                ctx.CloseAsync();
            }
        }

        /**
         * Handles client-side errors.
         */
        private class ClientErrorHandler : ChannelHandlerAdapter
        {
            private readonly NettyTransport transport;

            public ClientErrorHandler(NettyTransport transport)
            {
                this.transport = transport;
            }

            public override void ChannelInactive(IChannelHandlerContext ctx)
            {
                string remote = ctx.Channel.GetAttribute(SENDER).Get();
                LOG.LogDebug("Disconnected from server {0}", remote);

                // close incoming connection synchronously to make sure we don't receive
                // any more messages from this channel.
                if (transport.receivers.TryGetValue(remote, out var remoteChannel))
                {
                    remoteChannel.CloseAsync().Wait();
                }

                transport.receiver.OnDisconnected(remote);
            }

            public override void ExceptionCaught(IChannelHandlerContext ctx, Exception cause)
            {
                // Don't handle errors here. Call ctx.CloseAsync() and let ChannelInactive()
                // handle all the errors.
                LOG.LogDebug(cause, "Caught an exception");
                ctx.CloseAsync();
            }
        }

        public override void Send(string destination, Message message)
        {
            if (destination.Equals(hostPort))
            {
                // The message is being sent to itself. Don't bother going over TCP.
                // Directly call OnReceived.
                receiver.OnReceived(destination, message);
                return;
            }
            byte[] bytes = message.ToByteArray();
            if (senders.TryGetValue(destination, out var currentSender))
            {
                currentSender.requests.Add(bytes);
            }
            else
            {
                // no connection exists.
                LOG.LogDebug("No connection from {0} to {1}. Creating a new one",
                          hostPort, destination);
                Sender newSender = new Sender(this, hostPort, destination);
                if (senders.TryAdd(destination, newSender))
                {
                    newSender.requests.Add(bytes);
                    newSender.StartHandshake();
                }
                else
                {
                    senders[destination].requests.Add(bytes);
                }
            }
        }

        public override void Send(string destination, string filePath)
        {
            if (destination.Equals(hostPort))
            {
                LOG.LogError("Can't send file to itself.");
                throw new Exception("Can't send file to itself.");
            }
            FileInfo file = new FileInfo(filePath);
            if (senders.TryGetValue(destination, out var currentSender))
            {
                currentSender.requests.Add(file);
            }
            else
            {
                // no connection exists.
                LOG.LogDebug("No connection from {0} to {1}. Creating a new one",
                          hostPort, destination);
                Sender newSender = new Sender(this, hostPort, destination);
                if (senders.TryAdd(destination, newSender))
                {
                    newSender.requests.Add(file);
                    newSender.StartHandshake();
                }
                else
                {
                    senders[destination].requests.Add(file);
                }
            }
        }

        public override void Clear(string destination)
        {
            LOG.LogDebug("Closing the connection to {0}", destination);
            if (senders.TryRemove(destination, out var sender))
            {
                if (sender.channel != null)
                {
                    try
                    {
                        sender.channel.Pipeline.Remove("clientError");
                    }
                    catch (ArgumentException)
                    {
                        LOG.LogDebug("Channel pipeline has already been cleared");
                    }
                    sender.Shutdown();
                }
            }

            // close incoming connection synchronously to make sure we don't receive
            // any more messages from this channel.
            if (receivers.TryRemove(destination, out var receiverCtx))
            {
                receiverCtx.CloseAsync().Wait();
            }
        }

        /**
         * sender thread.
         */
        private class Sender
        {
            private readonly NettyTransport transport;
            readonly string destination;
            private Bootstrap bootstrap = null;
            public IChannel channel;
            Task future;
            public BlockingCollection<object> requests = new BlockingCollection<object>();
            private CancellationTokenSource cts = new CancellationTokenSource();

            public Sender(NettyTransport transport, string source, string destination)
            {
                this.transport = transport;
                this.destination = destination;
                bootstrap = new Bootstrap();
                bootstrap.Group(transport.workerGroup);
                bootstrap.Channel<TcpSocketChannel>();
                bootstrap.Option(ChannelOption.ConnectTimeout, TimeSpan.FromMilliseconds(1000));
                bootstrap.Option(ChannelOption.SoKeepalive, true);
                bootstrap.Option(ChannelOption.TcpNodelay, true);
                bootstrap.Handler(new ActionChannelInitializer<ISocketChannel>(ch =>
                {
                    if (transport.IsSslEnabled())
                    {
                        ch.Pipeline.AddLast(new TlsHandler(transport.clientCertificate, new TlsSettings(TlsRequirement.All, true, null, null)));
                    }
                    // Inbound handlers.
                    ch.Pipeline.AddLast("clientError", new ClientErrorHandler(transport));
                    // Outbound handlers.
                    ch.Pipeline.AddLast("frameEncoder", new LengthFieldPrepender(4));
                }));
            }

            public void StartHandshake()
            {
                string[] address = destination.Split(':', 2);
                string host = address[0];
                int port = int.Parse(address[1]);
                LOG.LogDebug("host: {0}, port: {1}", host, port);
                bootstrap.ConnectAsync(host, port).ContinueWith(t =>
                {
                    if (t.IsCompletedSuccessfully)
                    {
                        LOG.LogDebug("{0} connected to {1}. Sending a connection header",
                                  transport.hostPort, destination);
                        channel = t.Result;
                        // Associates the Sender with channel once the connection
                        // has been established.
                        channel.GetAttribute(SENDER).Set(destination);
                        Message msg = new Message
                        {
                            Type = Message.Types.MessageType.Handshake,
                            Handshake = new Handshake { NodeId = transport.hostPort }
                        };
                        byte[] bytes = msg.ToByteArray();
                        channel.WriteAndFlushAsync(Unpooled.WrappedBuffer(bytes));
                        Start();
                    }
                    else
                    {
                        LOG.LogDebug(t.Exception, "Failed to connect to {0}", destination);
                        Shutdown();
                        transport.receiver.OnDisconnected(destination);
                    }
                });
            }

            async Task SendFile(FileInfo file)
            {
                long length = file.Length;
                LOG.LogDebug("Got request of sending file {0} of length {1}.",
                          file.FullName, length);
                Message header = new Message
                {
                    Type = Message.Types.MessageType.FileHeader,
                    FileHeader = new FileHeader { Length = length }
                };
                byte[] bytes = header.ToByteArray();
                // Sends FILE_HEADER first before transferring actual file data
                await channel.WriteAndFlushAsync(Unpooled.WrappedBuffer(bytes));
                
                IChannelHandler prepender = channel.Pipeline.Get("frameEncoder");
                // Removes length prepender, we don't need this handler for file transferring.
                channel.Pipeline.Remove(prepender);
                // Adds ChunkedWriteHandler for file transferring.
                var cwh = new ChunkedWriteHandler();
                channel.Pipeline.AddLast(cwh);
                
                // Begins file transferring.
                if (channel.Pipeline.Get<TlsHandler>() != null)
                {
                    // Zero-Copy file transferring is not supported for ssl in Java Netty. 
                    // DotNetty's ChunkedFile equivalent.
                    await channel.WriteAndFlushAsync(new ChunkedFile(file.FullName));
                }
                else
                {
                    // Use Zero-Copy file transferring in non-ssl mode.
                    await channel.WriteAndFlushAsync(new DefaultFileRegion(new FileStream(file.FullName, FileMode.Open, FileAccess.Read), 0, length));
                }
                
                // Restores pipeline to original state.
                channel.Pipeline.Remove(cwh);
                channel.Pipeline.AddLast("frameEncoder", prepender);
            }

            public void Start()
            {
                future = Task.Run(async () =>
                {
                    LOG.LogDebug("Started the sender: {0} => {1}", transport.hostPort, destination);
                    try
                    {
                        foreach (var req in requests.GetConsumingEnumerable(cts.Token))
                        {
                            if (req is byte[] buf)
                            {
                                await channel.WriteAndFlushAsync(Unpooled.WrappedBuffer(buf));
                            }
                            else if (req is FileInfo file)
                            {
                                await SendFile(file);
                            }
                            else if (req is ShutdownCommand)
                            {
                                LOG.LogDebug("Got shutdown request.");
                                break;
                            }
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        LOG.LogDebug("Sender to {0} got cancelled", destination);
                    }
                    catch (Exception ex)
                    {
                        LOG.LogWarning(ex, "Sender failed with an exception");
                    }
                    finally
                    {
                        if (channel != null)
                        {
                            await channel.CloseAsync();
                        }
                    }
                });
            }

            public void Shutdown()
            {
                LOG.LogDebug("Shutting down the sender: {0} => {1}", transport.hostPort, destination);
                try
                {
                    requests.Add(new ShutdownCommand());
                    cts.Cancel();
                    future?.Wait(2000);
                    if (channel != null)
                    {
                        channel.CloseAsync().Wait();
                    }
                }
                catch (Exception ex)
                {
                    LOG.LogDebug(ex, "Ignore the exception during shutdown");
                }
            }

            class ShutdownCommand
            {
                // We use it to shutdown the sender thread.
            }
        }
    }
}
