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
import com.dangdang.ddframe.job.lite.internal.storage.JobNodePath;

/**
 * Elastic Job服务器节点名称的常量类.
 * 
 * @author zhangliang
 */
public class ServerNode {
    
    /**
     * 作业服务器信息根节点.
     */
    public static final String ROOT = "servers";
    
    static final String SEPARATOR = "_";
    
    static final String SERVER_BASE_NAME = ROOT + "/%s" + SEPARATOR;
    
    static final String SERVER_NAME = ROOT + "/%s";
    
    private final String jobName;
    
    private final JobNodePath jobNodePath;
    
    public ServerNode(final String jobName) {
        this.jobName = jobName;
        jobNodePath = new JobNodePath(jobName);
    }
    
    static String getServerBaseNode(final String ip) {
        return String.format(SERVER_BASE_NAME, ip);
    }
    static String getServerNode(final String serverName) {
        return String.format(SERVER_NAME, serverName);
    }
    
    /**
     * 从服务器路径中获取服务器名称
     * 
     * @param path 服务器路径
     * @return serverName 服务器名称
     */
    public static String getServerName(final String path) {
        String rootKey = ROOT + "/";
        int index = path.indexOf(rootKey);
        return index > 0 ? path.substring(path.indexOf(ROOT) + rootKey.length()) : null;
    }
    
    /**
     * 判断给定路径是否为作业当前服务器路径.
     * 
     * @param path 待判断的路径
     * @return 是否为作业当前服务器路径
     */
    public boolean isLocalJobPath(final String path) {
        return path.startsWith(jobNodePath.getFullPath(getServerNode(JobRegistry.getInstance().getJobServerName(jobName))));
    }
    
    /**
     * 判断给定路径是否为作业服务器路径.
     * 
     * @param path 待判断的路径
     * @return 是否为作业服务器路径
     */
    public boolean isServerJobPath(final String path) {
        String serverRoot = jobNodePath.getFullPath(ServerNode.ROOT);
        return path.startsWith(serverRoot) && !path.equals(serverRoot);
    }
    
}
