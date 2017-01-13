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

import com.dangdang.ddframe.job.config.JobCoreConfiguration;
import com.dangdang.ddframe.job.config.simple.SimpleJobConfiguration;
import com.dangdang.ddframe.job.lite.config.LiteJobConfiguration;
import com.dangdang.ddframe.job.lite.internal.config.ConfigurationService;
import com.dangdang.ddframe.job.lite.internal.execution.ExecutionService;
import com.dangdang.ddframe.job.lite.internal.schedule.JobRegistry;
import com.dangdang.ddframe.job.lite.internal.schedule.JobScheduleController;
import com.dangdang.ddframe.job.lite.internal.server.JobOperationListenerManager.ConnectionLostListener;
import com.dangdang.ddframe.job.lite.internal.server.JobOperationListenerManager.JobStatusJobListener;
import com.dangdang.ddframe.job.lite.internal.sharding.ShardingService;
import com.dangdang.ddframe.job.lite.internal.storage.JobNodeStorage;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.state.ConnectionState;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.unitils.util.ReflectionUtils;

import java.util.Arrays;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public final class JobOperationListenerManagerTest {
    
    @Mock
    private JobNodeStorage jobNodeStorage;
    
    @Mock
    private ServerService serverService;
    
    @Mock
    private ShardingService shardingService;
    
    @Mock
    private ExecutionService executionService;
    
    @Mock
    private JobScheduleController jobScheduleController;
    
    @Mock
    private ConfigurationService configService;
    
    private final JobOperationListenerManager jobOperationListenerManager = new JobOperationListenerManager(null, "test_job");
    
    @Before
    public void setUp() throws NoSuchFieldException {
        MockitoAnnotations.initMocks(this);
        ReflectionUtils.setFieldValue(jobOperationListenerManager, "serverService", serverService);
        ReflectionUtils.setFieldValue(jobOperationListenerManager, "shardingService", shardingService);
        ReflectionUtils.setFieldValue(jobOperationListenerManager, "executionService", executionService);
        ReflectionUtils.setFieldValue(jobOperationListenerManager, "configService", configService);
        ReflectionUtils.setFieldValue(jobOperationListenerManager, jobOperationListenerManager.getClass().getSuperclass().getDeclaredField("jobNodeStorage"), jobNodeStorage);
        JobRegistry.getInstance().addJobServerName("test_job", "host0_0001");
    }
    
    @Test
    public void assertStart() {
        jobOperationListenerManager.start();
        verify(jobNodeStorage).addConnectionStateListener(Matchers.<ConnectionLostListener>any());
        verify(jobNodeStorage).addDataListener(Matchers.<JobStatusJobListener>any());
    }
    
    @Test
    public void assertConnectionLostListenerWhenConnectionStateIsLost() {
        JobRegistry.getInstance().addJobScheduleController("test_job", jobScheduleController);
        jobOperationListenerManager.new ConnectionLostListener().stateChanged(null, ConnectionState.LOST);
        verify(jobScheduleController).pauseJob();
    }
    
    @Test
    public void assertConnectionLostListenerWhenConnectionStateIsReconnectedAndIsNotPausedManually() {
        JobRegistry.getInstance().addJobScheduleController("test_job", jobScheduleController);
        when(shardingService.getLocalHostShardingItems()).thenReturn(Arrays.asList(0, 1));
        when(serverService.isJobPausedManually()).thenReturn(false);
        when(configService.load(true)).thenReturn(createSimpleJobConfiguration());
        jobOperationListenerManager.new ConnectionLostListener().stateChanged(null, ConnectionState.RECONNECTED);
        verify(serverService).prepareServerNode(true);
        verify(configService).load(true);
        verify(shardingService).getLocalHostShardingItems();
        verify(executionService).clearRunningInfo(Arrays.asList(0, 1));
        verify(serverService).isJobPausedManually();
        verify(jobScheduleController).resumeJob();
    }
    
    private LiteJobConfiguration createSimpleJobConfiguration() {
        JobCoreConfiguration simpleCoreConfig = JobCoreConfiguration.newBuilder("test_job", "0/5 * * * * ?", 5).build();
        SimpleJobConfiguration simpleJobConfig = new SimpleJobConfiguration(simpleCoreConfig, "com.dangdang.test.MyJob");
        LiteJobConfiguration simpleJobRootConfig = LiteJobConfiguration.newBuilder(simpleJobConfig).overwrite(true).build();
        return simpleJobRootConfig;
    }
    
    @Test
    public void assertConnectionLostListenerWhenConnectionStateIsReconnectedAndIsPausedManually() {
        JobRegistry.getInstance().addJobScheduleController("test_job", jobScheduleController);
        when(shardingService.getLocalHostShardingItems()).thenReturn(Arrays.asList(0, 1));
        when(serverService.isJobPausedManually()).thenReturn(true);
        when(configService.load(true)).thenReturn(createSimpleJobConfiguration());
        jobOperationListenerManager.new ConnectionLostListener().stateChanged(null, ConnectionState.RECONNECTED);
        verify(serverService).prepareServerNode(true);
        verify(configService).load(true);
        verify(shardingService).getLocalHostShardingItems();
        verify(executionService).clearRunningInfo(Arrays.asList(0, 1));
        verify(serverService).isJobPausedManually();
        verify(jobScheduleController, times(0)).resumeJob();
    }
    
    @Test
    public void assertConnectionLostListenerWhenConnectionStateIsOther() {
        JobRegistry.getInstance().addJobScheduleController("test_job", jobScheduleController);
        jobOperationListenerManager.new ConnectionLostListener().stateChanged(null, ConnectionState.CONNECTED);
        verify(jobScheduleController, times(0)).pauseJob();
        verify(jobScheduleController, times(0)).resumeJob();
    }
    
    @Test
    public void assertJobStatusJobListenerWhenRemove() {
        jobOperationListenerManager.new JobStatusJobListener().dataChanged(null, new TreeCacheEvent(
                TreeCacheEvent.Type.NODE_REMOVED, new ChildData("/test_job/servers/host0_0001", null, "".getBytes())), "/test_job/servers/host0_0001");
        verify(serverService, times(0)).loadServerData();
        verify(jobScheduleController, times(0)).triggerJob();
        verify(jobScheduleController, times(0)).shutdown();
        verify(jobScheduleController, times(0)).pauseJob();
        verify(jobScheduleController, times(0)).resumeJob();
    }
    
    @Test
    public void assertJobStatusJobListenerWhenUpdateButNotLocalHostJobPath() {
        jobOperationListenerManager.new JobStatusJobListener().dataChanged(null, new TreeCacheEvent(
                TreeCacheEvent.Type.NODE_UPDATED, new ChildData("/test_job/servers/host1_0001", null, "".getBytes())), "/test_job/servers/host1_0001");
        verify(serverService, times(0)).loadServerData();
        verify(jobScheduleController, times(0)).triggerJob();
        verify(jobScheduleController, times(0)).shutdown();
        verify(jobScheduleController, times(0)).pauseJob();
        verify(jobScheduleController, times(0)).resumeJob();
    }
    
    @Test
    public void assertJobStatusJobListenerWhenTriggerStatusUpdateButNoJobScheduleController() {
        ServerData data = new ServerData("host0", "host0", false);
        data.markTriggered();
        when(serverService.loadServerData()).thenReturn(data);
        jobOperationListenerManager.new JobStatusJobListener().dataChanged(null, new TreeCacheEvent(
                TreeCacheEvent.Type.NODE_UPDATED, new ChildData("/test_job/servers/host0_0001", null, "".getBytes())), "/test_job/servers/host0_0001");
        verify(serverService).loadServerData();
        verify(serverService).clearJobTriggerStatus();
        verify(jobScheduleController, times(0)).triggerJob();
        verify(jobScheduleController, times(0)).shutdown();
        verify(jobScheduleController, times(0)).pauseJob();
        verify(jobScheduleController, times(0)).resumeJob();
    }
    
    @Test
    public void assertJobStatusJobListenerWhenTriggerStatusUpdateButServerIsNotReady() {
        JobRegistry.getInstance().addJobScheduleController("test_job", jobScheduleController);
        ServerData data = new ServerData("host0", "host0", false);
        data.markTriggered();
        data.setPaused(true);
        when(serverService.loadServerData()).thenReturn(data);
        jobOperationListenerManager.new JobStatusJobListener().dataChanged(null, new TreeCacheEvent(
                TreeCacheEvent.Type.NODE_UPDATED, new ChildData("/test_job/servers/host0_0001", null, "".getBytes())), "/test_job/servers/host0_0001");
        verify(serverService).loadServerData();
        verify(serverService).clearJobTriggerStatus();
        verify(serverService).isServerReady();
        verify(jobScheduleController, times(0)).triggerJob();
        verify(jobScheduleController, times(0)).shutdown();
        verify(jobScheduleController, times(0)).pauseJob();
        verify(jobScheduleController, times(0)).resumeJob();
    }
    
    @Test
    public void assertJobStatusJobListenerWhenTriggerStatusUpdate() {
        JobRegistry.getInstance().addJobScheduleController("test_job", jobScheduleController);
        ServerData data = new ServerData("host0", "host0", false);
        data.markTriggered();
        when(serverService.loadServerData()).thenReturn(data);
        when(serverService.isServerReady()).thenReturn(true);
        jobOperationListenerManager.new JobStatusJobListener().dataChanged(null, new TreeCacheEvent(
                TreeCacheEvent.Type.NODE_UPDATED, new ChildData("/test_job/servers/host0_0001", null, "".getBytes())), "/test_job/servers/host0_0001");
        verify(serverService).loadServerData();
        verify(serverService).clearJobTriggerStatus();
        verify(serverService).isServerReady();
        verify(jobScheduleController).triggerJob();
        verify(jobScheduleController, times(0)).shutdown();
        verify(jobScheduleController, times(0)).pauseJob();
        verify(jobScheduleController, times(0)).resumeJob();
    }
    
    @Test
    public void assertJobStatusJobListenerWhenStatusUpdateButJobIsNotExisted() {
        JobRegistry.getInstance().addJobScheduleController("test_job", jobScheduleController);
        when(serverService.loadServerData()).thenReturn(null);
        jobOperationListenerManager.new JobStatusJobListener().dataChanged(null, new TreeCacheEvent(
                TreeCacheEvent.Type.NODE_UPDATED, new ChildData("/test_job/servers/host0_0001", null, "".getBytes())), "/test_job/servers/host0_0001");
        verify(serverService).loadServerData();
        verify(serverService, times(0)).clearJobTriggerStatus();
        verify(serverService, times(0)).isServerReady();
        verify(jobScheduleController, times(0)).triggerJob();
        verify(jobScheduleController, times(0)).shutdown();
        verify(jobScheduleController, times(0)).pauseJob();
        verify(jobScheduleController, times(0)).resumeJob();
    }
    
    
    @Test
    public void assertJobStatusJobListenerWhenPausedStatusUpdateButNoMark() {
        JobRegistry.getInstance().addJobScheduleController("test_job", jobScheduleController);
        ServerData data = new ServerData("host0", "host0", false);
        data.setPaused(true);
        when(serverService.loadServerData()).thenReturn(data);
        jobOperationListenerManager.new JobStatusJobListener().dataChanged(null, new TreeCacheEvent(
                TreeCacheEvent.Type.NODE_UPDATED, new ChildData("/test_job/servers/host0_0001", null, "".getBytes())), "/test_job/servers/host0_0001");
        verify(serverService).loadServerData();
        verify(serverService, times(0)).clearJobTriggerStatus();
        verify(serverService, times(0)).isServerReady();
        verify(jobScheduleController, times(0)).triggerJob();
        verify(jobScheduleController, times(0)).shutdown();
        verify(jobScheduleController, times(0)).pauseJob();
        verify(jobScheduleController, times(0)).resumeJob();
    }
    
    @Test
    public void assertJobStatusJobListenerWhenPausedStatusUpdate() {
        JobRegistry.getInstance().addJobScheduleController("test_job", jobScheduleController);
        ServerData data = new ServerData("host0", "host0", false);
        data.markPaused();
        when(serverService.loadServerData()).thenReturn(data);
        jobOperationListenerManager.new JobStatusJobListener().dataChanged(null, new TreeCacheEvent(
                TreeCacheEvent.Type.NODE_UPDATED, new ChildData("/test_job/servers/host0_0001", null, "".getBytes())), "/test_job/servers/host0_0001");
        verify(serverService).loadServerData();
        verify(serverService, times(0)).clearJobTriggerStatus();
        verify(serverService, times(0)).isServerReady();
        verify(jobScheduleController, times(0)).triggerJob();
        verify(jobScheduleController, times(0)).shutdown();
        verify(jobScheduleController).pauseJob();
        verify(jobScheduleController, times(0)).resumeJob();
    }
    
    @Test
    public void assertJobStatusJobListenerWhenPausedStatusUpdateWhenIsJobPaused() {
        JobRegistry.getInstance().addJobScheduleController("test_job", jobScheduleController);
        ServerData data = new ServerData("host0", "host0", false);
        data.markResumed();
        when(serverService.loadServerData()).thenReturn(data);
        jobOperationListenerManager.new JobStatusJobListener().dataChanged(null, new TreeCacheEvent(
                TreeCacheEvent.Type.NODE_UPDATED, new ChildData("/test_job/servers/host0_0001", null, "".getBytes())), "/test_job/servers/host0_0001");
        verify(serverService).loadServerData();
        verify(serverService, times(0)).clearJobTriggerStatus();
        verify(serverService, times(0)).isServerReady();
        verify(jobScheduleController, times(0)).triggerJob();
        verify(jobScheduleController, times(0)).shutdown();
        verify(jobScheduleController, times(0)).pauseJob();
        verify(jobScheduleController).resumeJob();
    }
    
    @Test
    public void assertJobStatusJobListenerWhenShutdownStatusUpdate() {
        JobRegistry.getInstance().addJobScheduleController("test_job", jobScheduleController);
        ServerData data = new ServerData("host0", "host0", false);
        data.markShutdown();
        when(serverService.loadServerData()).thenReturn(data);
        jobOperationListenerManager.new JobStatusJobListener().dataChanged(null, new TreeCacheEvent(
                TreeCacheEvent.Type.NODE_UPDATED, new ChildData("/test_job/servers/host0_0001", null, "".getBytes())), "/test_job/servers/host0_0001");
        verify(serverService).loadServerData();
        verify(serverService, times(0)).clearJobTriggerStatus();
        verify(serverService, times(0)).isServerReady();
        verify(jobScheduleController, times(0)).triggerJob();
        verify(jobScheduleController).shutdown();
        verify(jobScheduleController, times(0)).pauseJob();
        verify(jobScheduleController, times(0)).resumeJob();
    }
}
