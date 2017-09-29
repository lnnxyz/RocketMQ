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

import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;

/**
 * 吊钩事件
 */
public interface RPCHook {

    /**
     * 请求之前处理
     * @param remoteAddr 远处地址
     * @param request 请求对象
     */
    void doBeforeRequest(final String remoteAddr, final RemotingCommand request);

    /**
     * 响应之后处理
     * @param remoteAddr  远程地址
     * @param request   请求对象
     * @param response  响应对象
     */
    void doAfterResponse(final String remoteAddr, final RemotingCommand request,
                         final RemotingCommand response);
}
