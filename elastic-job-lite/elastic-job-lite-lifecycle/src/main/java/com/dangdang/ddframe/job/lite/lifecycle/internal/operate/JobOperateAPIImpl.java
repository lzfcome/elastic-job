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

package com.dangdang.ddframe.job.lite.lifecycle.internal.operate;

import com.dangdang.ddframe.job.lite.internal.server.ServerData;
import com.dangdang.ddframe.job.lite.internal.server.ServerService;
import com.dangdang.ddframe.job.lite.internal.storage.JobNodePath;
import com.dangdang.ddframe.job.lite.lifecycle.api.JobOperateAPI;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import com.google.common.base.Optional;

import java.util.Collection;

/**
 * 操作作业的实现类.
 *
 * @author zhangliang
 */
public final class JobOperateAPIImpl implements JobOperateAPI {
    
    private final CoordinatorRegistryCenter regCenter;
    
    private final JobOperateTemplate jobOperatorTemplate;
    
    public JobOperateAPIImpl(final CoordinatorRegistryCenter regCenter) {
        this.regCenter = regCenter;
        jobOperatorTemplate = new JobOperateTemplate(regCenter);
    }
    
    private abstract class AbstractJobStatusOperateCallback implements JobOperateCallback {
    
        @Override
        public boolean doOperate(String jobName, String serverName) {
            ServerService serverService = new ServerService(regCenter, jobName);
            ServerData data = serverService.loadServerData(serverName);
            changeStatus(data);
            serverService.updateServerData(serverName, data);
            return true;
        }
        
        protected abstract void changeStatus(final ServerData data);
        
    }
    
    @Override
    public void trigger(final Optional<String> jobName, final Optional<String> serverName) {
        jobOperatorTemplate.operate(jobName, serverName, new AbstractJobStatusOperateCallback() {
            @Override
            protected void changeStatus(ServerData data) {
                data.markTriggered();
            }
        });
    }
    
    @Override
    public void pause(final Optional<String> jobName, final Optional<String> serverName) {
        jobOperatorTemplate.operate(jobName, serverName, new AbstractJobStatusOperateCallback() {
            @Override
            protected void changeStatus(ServerData data) {
                data.markPaused();
            }
        });
    }
    
    @Override
    public void resume(final Optional<String> jobName, final Optional<String> serverName) {
        jobOperatorTemplate.operate(jobName, serverName, new AbstractJobStatusOperateCallback() {
            @Override
            protected void changeStatus(ServerData data) {
                data.markResumed();
            }
        });
    }
    
    @Override
    public void disable(final Optional<String> jobName, final Optional<String> serverName) {
        jobOperatorTemplate.operate(jobName, serverName, new AbstractJobStatusOperateCallback() {
            @Override
            protected void changeStatus(ServerData data) {
                data.markDisabled();
            }
        });
    }
    
    @Override
    public void enable(final Optional<String> jobName, final Optional<String> serverName) {
        jobOperatorTemplate.operate(jobName, serverName, new AbstractJobStatusOperateCallback() {
            @Override
            protected void changeStatus(ServerData data) {
                data.markEnabled();
            }
        });
    }
    
    @Override
    public void shutdown(final Optional<String> jobName, final Optional<String> serverName) {
        jobOperatorTemplate.operate(jobName, serverName, new AbstractJobStatusOperateCallback() {
            @Override
            protected void changeStatus(ServerData data) {
                data.markShutdown();
            }
        });
    }
    
    @Override
    public Collection<String> remove(final Optional<String> jobName, final Optional<String> serverName) {
        return jobOperatorTemplate.operate(jobName, serverName, new JobOperateCallback() {
            
            @Override
            public boolean doOperate(final String jobName, final String serverName) {
                JobNodePath jobNodePath = new JobNodePath(jobName);
                ServerService serverService = new ServerService(regCenter, jobName);
                //TODO 这里有逻辑上的问题
                if (regCenter.isExisted(jobNodePath.getServerNodePath(serverName)) || regCenter.isExisted(jobNodePath.getLeaderHostNodePath())) {
                    return false;
                }
                serverService.removeServerData(serverName);
                if (0 == regCenter.getNumChildren(jobNodePath.getServerNodePath())) {
                    regCenter.remove("/" + jobName);
                }
                return true;
            }
        });
    }
}
