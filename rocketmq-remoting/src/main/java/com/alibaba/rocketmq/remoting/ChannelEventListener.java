/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.alibaba.rocketmq.remoting;

import io.netty.channel.Channel;


/**
 * 通道信息监听器
 */
public interface ChannelEventListener {

    /**
     * 连接事件
     * @param remoteAddr
     * @param channel
     */
    void onChannelConnect(final String remoteAddr, final Channel channel);

    /**
     * 关闭事件
     * @param remoteAddr
     * @param channel
     */
    void onChannelClose(final String remoteAddr, final Channel channel);

    /**
     * 异常事件
     * @param remoteAddr
     * @param channel
     */
    void onChannelException(final String remoteAddr, final Channel channel);


    void onChannelIdle(final String remoteAddr, final Channel channel);
}
