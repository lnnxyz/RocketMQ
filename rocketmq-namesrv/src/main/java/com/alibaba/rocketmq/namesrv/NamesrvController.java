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
package com.alibaba.rocketmq.namesrv;

import com.alibaba.rocketmq.common.ThreadFactoryImpl;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.namesrv.NamesrvConfig;
import com.alibaba.rocketmq.namesrv.kvconfig.KVConfigManager;
import com.alibaba.rocketmq.namesrv.processor.ClusterTestRequestProcessor;
import com.alibaba.rocketmq.namesrv.processor.DefaultRequestProcessor;
import com.alibaba.rocketmq.namesrv.routeinfo.BrokerHousekeepingService;
import com.alibaba.rocketmq.namesrv.routeinfo.RouteInfoManager;
import com.alibaba.rocketmq.remoting.RemotingServer;
import com.alibaba.rocketmq.remoting.netty.NettyRemotingServer;
import com.alibaba.rocketmq.remoting.netty.NettyServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Namesrv 服务控制类
 */
public class NamesrvController {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.NamesrvLoggerName);

    private final NamesrvConfig namesrvConfig;

    private final NettyServerConfig nettyServerConfig;

    /**
     * 单线程池，此线程用来启动namesrc，启动之后还有2个定时线程来scanNotActiveBroker（清理不生效broker）和printAllPeriodically（打印每个namesrv的配置表）
     */
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
            "NSScheduledThread"));

    /**
     * namesrv配置管理器，容器为HashMap<String(Namespace), HashMap<String(Key), String(Value)>> configTable，ReadWriteLock保证读写安全
     */
    private final KVConfigManager kvConfigManager;

    //所有运行数据管理器，topicqueuetable、brokeraddrtable等，信息量很多，ReadWriteLock保证读写安全
    private final RouteInfoManager routeInfoManager;

    //服务启动接口，这里传入的是NettyRemotingServer，用netty启动，是rocketmq remoting模块，包括注册请求处理器DefaultRequestProcessor，以及几种数据传输方式invokeSync、invokeAsync、invokeOneway等
    private RemotingServer remotingServer;

    //Broker事件监听器，属于netty概念，监听chanel 4个动作事件，提供处理方法
    private BrokerHousekeepingService brokerHousekeepingService;

    //remotingServer的并发处理器，处理各种类型请求
    private ExecutorService remotingExecutor;


    public NamesrvController(NamesrvConfig namesrvConfig, NettyServerConfig nettyServerConfig) {
        this.namesrvConfig = namesrvConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.kvConfigManager = new KVConfigManager(this);
        this.routeInfoManager = new RouteInfoManager();
        this.brokerHousekeepingService = new BrokerHousekeepingService(this);
    }

    //服务控制器 初始化
    public boolean initialize() {
        //加载kvConfig.json至KVConfigManager的configTable，即持久化转移到内存
        this.kvConfigManager.load();
        //将namesrv作为一个netty server启动
        this.remotingServer = new NettyRemotingServer(this.nettyServerConfig, this.brokerHousekeepingService);
        //启动请求处理线程池
        this.remotingExecutor =
                Executors.newFixedThreadPool(nettyServerConfig.getServerWorkerThreads(), new ThreadFactoryImpl("RemotingExecutorThread_"));
        //注册默认DefaultRequestProcessor和remotingExecutor，等start启动即开始处理netty请求
        this.registerProcessor();

        //每10s检查2分钟接受不到心跳的broker清除掉
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                NamesrvController.this.routeInfoManager.scanNotActiveBroker();
            }
        }, 5, 10, TimeUnit.SECONDS);
        //每10分钟，打印namesrv全局配置信息
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                NamesrvController.this.kvConfigManager.printAllPeriodically();
            }
        }, 1, 10, TimeUnit.MINUTES);

        return true;
    }


    private void registerProcessor() {
        if (namesrvConfig.isClusterTest()) {

            this.remotingServer.registerDefaultProcessor(new ClusterTestRequestProcessor(this, namesrvConfig.getProductEnvName()),
                    this.remotingExecutor);
        } else {

            this.remotingServer.registerDefaultProcessor(new DefaultRequestProcessor(this), this.remotingExecutor);
        }
    }

    /**
     * 服务启动
     * @throws Exception
     */
    public void start() throws Exception {
        this.remotingServer.start();
    }

    /**
     * 服务关闭
     */
    public void shutdown() {
        this.remotingServer.shutdown();
        this.remotingExecutor.shutdown();
        this.scheduledExecutorService.shutdown();
    }

    /**
     * 获取名称服务配置对象
     * @return
     */
    public NamesrvConfig getNamesrvConfig() {
        return namesrvConfig;
    }

    /**
     * 获取通信服务配置对象
     * @return
     */
    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }


    public KVConfigManager getKvConfigManager() {
        return kvConfigManager;
    }


    public RouteInfoManager getRouteInfoManager() {
        return routeInfoManager;
    }


    public RemotingServer getRemotingServer() {
        return remotingServer;
    }


    public void setRemotingServer(RemotingServer remotingServer) {
        this.remotingServer = remotingServer;
    }
}
