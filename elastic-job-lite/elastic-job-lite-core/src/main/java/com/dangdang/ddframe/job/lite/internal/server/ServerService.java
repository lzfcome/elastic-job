/*
 * Copyright 1999-2015 dangdang.com.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package com.dangdang.ddframe.job.lite.internal.server;

import com.dangdang.ddframe.job.lite.internal.schedule.JobRegistry;
import com.dangdang.ddframe.job.lite.internal.storage.JobNodeStorage;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import com.dangdang.ddframe.job.util.env.LocalHostService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Strings;

/**
 * 作业服务器节点服务.
 * 
 * @author zhangliang
 * @author caohao
 */
public class ServerService {
    
    private final String jobName;
    
    private final JobNodeStorage jobNodeStorage;
    
    private final LocalHostService localHostService = new LocalHostService();
    
    private final JobRegistry jobRegistry;
    
    public ServerService(final CoordinatorRegistryCenter regCenter, final String jobName) {
        this.jobName = jobName;
        jobNodeStorage = new JobNodeStorage(regCenter, jobName);
        jobRegistry = JobRegistry.getInstance();
    }
    
    /**
     * 初始化作业server节点.
     * 
     * @param enabled 是否启用job
     */
    public void prepareServerNode(final boolean enabled) {
        createServerData(enabled);
    }
    
    /**
     * 修复作业server节点.
     * 
     * @param enabled 是否启用job
     */
    public void repairServerNode(final boolean enabled) {
        if (!jobNodeStorage.isJobNodeExisted(ServerNode.getServerNode(jobRegistry.getJobServerName(jobName)))) {
            createServerData(enabled);
        } else {
            ServerData data = loadServerData();
            if (null != data) {
                data.setStatus(ServerStatus.READY);
            } else {
                data = new ServerData(localHostService.getHostName(), localHostService.getIp(), !enabled);
            }
            updateServerData(data);
        }
    }
    
    private void createServerData(final boolean enabled) {
        ServerData data = new ServerData(localHostService.getHostName(), localHostService.getIp(), !enabled);
        String serverNodePath = jobNodeStorage.fillEphemeralSequentialJobNode(ServerNode.getServerBaseNode(localHostService.getIp()), ServerDataGsonFactory.toJson(data));
        if (!Strings.isNullOrEmpty(serverNodePath)) {
            jobRegistry.addJobServerName(jobName, serverNodePath.substring(serverNodePath.indexOf(localHostService.getIp())));
        }
    }
    
    /**
     * 清除作业状态变更的标记.
     */
    public void clearJobStatusMark() {
        ServerData data = loadServerData();
        if (data == null) {
            return;
        }
        data.setChangedItem(null);
        updateServerData(data);
    }
    
    /**
     * 判断是否是手工暂停的作业.
     * 
     * @return 是否是手工暂停的作业
     */
    public boolean isJobPausedManually() {
        ServerData data = loadServerData();
        if (data == null) {
            return false;
        }
        return data.isPaused();
    }
    
    /**
     * 在开始或结束执行作业时更新服务器状态.
     * 
     * @param status 服务器状态
     */
    public void updateServerStatus(final ServerStatus status) {
        ServerData data = loadServerData();
        if (data == null) {
            return;
        }
        data.setStatus(status);
        updateServerData(data);
    }
    
    /**
     * 获取所有的作业服务器列表.
     * 
     * @return 所有的作业服务器列表
     */
    public List<String> getAllServers() {
        List<String> result = jobNodeStorage.getJobNodeChildrenKeys(ServerNode.ROOT);
        Collections.sort(result);
        return result;
    }
    
    /**
     * 获取可分片的作业服务器列表.
     *
     * @return 可分片的作业服务器列表
     */
    public List<String> getAvailableShardingServers() {
        List<String> servers = getAllServers();
        List<String> result = new ArrayList<>(servers.size());
        for (String each : servers) {
            if (isShardingServerAvailable(each)) {
                result.add(each);
            }
        }
        return result;
    }
    
    /**
     * 判断作业服务器是否可分片.
     * 
     * @param serverName 作业服务器名称
     * @return 作业服务器是否可分片
     */
    public boolean isShardingServerAvailable(final String serverName) {
        ServerData data = loadServerData(serverName);
        return data != null && !data.isDisabled() && !data.isShutdown();
    }
    
    /**
     * 获取可用的作业服务器列表.
     * 
     * @return 可用的作业服务器列表
     */
    public List<String> getAvailableServers() {
        List<String> servers = getAllServers();
        List<String> result = new ArrayList<>(servers.size());
        for (String each : servers) {
            if (isServerAvailable(each)) {
                result.add(each);
            }
        }
        return result;
    }
    
    /**
     * 判断作业服务器是否可用.
     * 
     * @return 作业服务器是否可用
     */
    public boolean isServerAvailable() {
        return isServerAvailable(jobRegistry.getJobServerName(jobName));
    }
    
    /**
     * 判断作业服务器是否可用.
     * 
     * @param serverName 作业服务器名称
     * @return 作业服务器是否可用
     */
    public boolean isServerAvailable(final String serverName) {
        ServerData data = loadServerData(serverName);
        return data != null && !data.isDisabled() && !data.isPaused() && !data.isShutdown();
    }
    
    /**
     * 判断当前服务器是否是等待执行的状态.
     * 
     * @return 当前服务器是否是等待执行的状态
     */
    public boolean isServerReady() {
        ServerData data = loadServerData(jobRegistry.getJobServerName(jobName));
        return data != null && !data.isDisabled() && !data.isPaused() && !data.isShutdown() && ServerStatus.READY == data.getStatus();
    }
    
    /**
     * 加载当前服务器数据.
     * 
     * @return 当前服务器数据
     */
    public ServerData loadServerData(){
        return loadServerData(jobRegistry.getJobServerName(jobName));
    }
    
    /**
     * 加载指定服务器数据.
     * 
     * @param serverName 指定服务器名称
     * @return 指定服务器数据
     */
    public ServerData loadServerData(final String serverName) {
        return ServerDataGsonFactory.fromJson(jobNodeStorage.getJobNodeData(ServerNode.getServerNode(serverName)));
    }
    
    /**
     * 更新当前服务器数据
     * 
     * @param data 服务器数据
     */
    public void updateServerData(final ServerData data){
        updateServerData(jobRegistry.getJobServerName(jobName), data);
    }
    
    /**
     * 更新指定服务器数据
     * 
     * @param serverName 服务器名称
     * @param data 服务器数据
     */
    public void updateServerData(final String serverName, final ServerData data){
        // TODO 有没有并发问题
        jobNodeStorage.updateJobNode(ServerNode.getServerNode(serverName), ServerDataGsonFactory.toJson(data));
    }
    
    public void removeServerData(){
        removeServerData(jobRegistry.getJobServerName(jobName));
    }
    
    public void removeServerData(final String serverName){
        jobNodeStorage.removeJobNodeIfExisted(ServerNode.getServerNode(serverName));
    }
    
    /**
     * 判断服务器是否变为不可用状态.
     * 
     * <p>此时需要重新分片</p>
     * 
     * @param serverName 服务器名称
     * @return 服务器是否变为不可用状态
     */
    public boolean isShardingServerOff(final String serverName) {
        ServerData data = loadServerData(serverName);
        return data == null || data.isDisabledWithMark() || data.isShutdownWithMark();
    }
    
    /**
     * 判断服务器是否变为可用状态.
     * 
     * <p>此时需要重新分片</p>
     * 
     * @param serverName 服务器名称
     * @return 服务器是否变为不可用状态
     */
    public boolean isShardingServerOn(final String serverName) {
        ServerData data = loadServerData(serverName);
        return data != null && data.isEnabledWithMark();
    }
    
    /**
     * 判断任务当前服务器是否变为停止运行状态
     * 
     * @return 当前任务服务器是否变为停止运行状态
     */
    public boolean isServerOff() {
        return isServerOff(jobRegistry.getJobServerName(jobName));
    }
    
    /**
     * 判断服务器是否变为停止运行状态
     * 
     * @param serverName 服务器名称
     * @return 服务器是否变为停止运行状态
     */
    public boolean isServerOff(final String serverName) {
        ServerData data = loadServerData(serverName);
        return data == null || data.isDisabledWithMark() || data.isPausedWithMark() || data.isShutdownWithMark();
    }
    
    /**
     * 判断任务当前服务器是否变为可运行状态
     * 
     * @return 当前任务服务器是否变为停止运行状态
     */
    public boolean isServerOn() {
        return isServerOn(jobRegistry.getJobServerName(jobName));
    }
    
    /**
     * 判断服务器是否变为可运行状态
     * 
     * @param serverName 服务器名称
     * @return 服务器是否变为停止运行状态
     */
    public boolean isServerOn(final String serverName) {
        ServerData data = loadServerData(serverName);
        return data != null && (data.isEnabledWithMark() || data.isResumedWithMark());
    }
    
}
