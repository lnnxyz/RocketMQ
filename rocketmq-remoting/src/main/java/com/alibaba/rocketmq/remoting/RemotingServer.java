/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.remoting;

import com.alibaba.rocketmq.remoting.common.Pair;
import com.alibaba.rocketmq.remoting.exception.RemotingSendRequestException;
import com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;
import com.alibaba.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import com.alibaba.rocketmq.remoting.netty.NettyRequestProcessor;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import io.netty.channel.Channel;

import java.util.concurrent.ExecutorService;


/**
 * 远程通信服务
 */
public interface RemotingServer extends RemotingService {

    /**
     * 注册请求处理器
     * @param requestCode   标识码
     * @param processor     处理器
     * @param executor      服务
     */
    void registerProcessor(final int requestCode, final NettyRequestProcessor processor,
                           final ExecutorService executor);

    /**
     * 注册默认处理器和执行服务
     * @param processor
     * @param executor
     */
    void registerDefaultProcessor(final NettyRequestProcessor processor, final ExecutorService executor);

    /**
     * 本地监听端口
     * @return
     */
    int localListenPort();

    /**
     * 获取 处理器和执行服务  对
     * @param requestCode
     * @return
     */
    Pair<NettyRequestProcessor, ExecutorService> getProcessorPair(final int requestCode);

    /**
     * 处理同步请求
     * @param channel  通道
     * @param request  请求
     * @param timeoutMillis 超时时间
     * @return
     * @throws InterruptedException
     * @throws RemotingSendRequestException
     * @throws RemotingTimeoutException
     */
    RemotingCommand invokeSync(final Channel channel, final RemotingCommand request,
                               final long timeoutMillis) throws InterruptedException, RemotingSendRequestException,
            RemotingTimeoutException;

    /**
     * 处理异步请求
     * @param channel 通道
     * @param request 请求
     * @param timeoutMillis 超时时间
     * @param invokeCallback 回调
     * @throws InterruptedException
     * @throws RemotingTooMuchRequestException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     */
    void invokeAsync(final Channel channel, final RemotingCommand request, final long timeoutMillis,
                     final InvokeCallback invokeCallback) throws InterruptedException,
            RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;

    /**
     * 处理顺序请求
     * @param channel   通道
     * @param request   请求
     * @param timeoutMillis 超时时间
     * @throws InterruptedException
     * @throws RemotingTooMuchRequestException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     */
    void invokeOneway(final Channel channel, final RemotingCommand request, final long timeoutMillis)
            throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException,
            RemotingSendRequestException;

}
