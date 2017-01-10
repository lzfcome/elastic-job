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
import com.dangdang.ddframe.job.util.env.LocalHostService;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.unitils.util.ReflectionUtils;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public final class ServerServiceTest {
    
    @Mock
    private JobNodeStorage jobNodeStorage;
    
    @Mock
    private LocalHostService localHostService;
    
    private final ServerService serverService = new ServerService(null, "test_job");
    
    @Before
    public void setUp() throws NoSuchFieldException {
        MockitoAnnotations.initMocks(this);
        ReflectionUtils.setFieldValue(serverService, "jobNodeStorage", jobNodeStorage);
        ReflectionUtils.setFieldValue(serverService, "localHostService", localHostService);
        when(localHostService.getIp()).thenReturn("mockedIP");
        when(localHostService.getHostName()).thenReturn("mockedHostName");
    }
    
    @Test
    public void assertPrepareServerNode() {
        serverService.prepareServerNode(true);
        ServerData data = new ServerData("mockedHostName", "mockedIP", false);
        verify(jobNodeStorage).fillEphemeralSequentialJobNode("servers/mockedIP_", ServerDataGsonFactory.toJson(data));
        verify(localHostService, times(2)).getIp();
        verify(localHostService).getHostName();
    }
    
    @Test
    public void assertPrepareServerNodeWhenConnectionStateIsReconnectedAndIsJobNodeNotExisted() {
        JobRegistry.getInstance().addJobServerName("test_job", "mockedIP_0001");
        serverService.prepareServerNode(true);
        verify(jobNodeStorage).isJobNodeExisted("servers/mockedIP_0001");
        ServerData data = new ServerData("mockedHostName", "mockedIP", false);
        verify(jobNodeStorage).fillEphemeralSequentialJobNode("servers/mockedIP_", ServerDataGsonFactory.toJson(data));
        verify(localHostService, times(2)).getIp();
        verify(localHostService).getHostName();
    }
    
    @Test
    public void assertPrepareServerNodeWhenConnectionStateIsReconnectedAndIsJobNodeExisted() {
        JobRegistry.getInstance().addJobServerName("test_job", "mockedIP_0001");
        when(jobNodeStorage.isJobNodeExisted("servers/mockedIP_0001")).thenReturn(true);
        serverService.prepareServerNode(true);
        verify(jobNodeStorage).isJobNodeExisted("servers/mockedIP_0001");
        verify(jobNodeStorage).getJobNodeData("servers/mockedIP_0001");
        ServerData data = new ServerData("mockedHostName", "mockedIP", false);
        jobNodeStorage.updateJobNode("servers/mockedIP_0001", ServerDataGsonFactory.toJson(data));
        verify(localHostService).getIp();
        verify(localHostService).getHostName();
    }
    
    @Test
    public void assertClearJobTriggerStatus() {
        JobRegistry.getInstance().addJobServerName("test_job", "mockedIP_0001");
        ServerData data = new ServerData("mockedHostName", "mockedIP", false);
        data.setTriggerAndMark(true);
        when(jobNodeStorage.getJobNodeData("servers/mockedIP_0001")).thenReturn(ServerDataGsonFactory.toJson(data));
        serverService.clearJobTriggerStatus();
        verify(jobNodeStorage).getJobNodeData("servers/mockedIP_0001");
        data.setTriggerAndRemoveMark(false);
        verify(jobNodeStorage).updateJobNode("servers/mockedIP_0001", ServerDataGsonFactory.toJson(data));
    }
    
    @Test
    public void assertClearJobPausedStatus() {
        JobRegistry.getInstance().addJobServerName("test_job", "mockedIP_0001");
        ServerData data = new ServerData("mockedHostName", "mockedIP", false);
        data.setPausedAndMark(true);
        when(jobNodeStorage.getJobNodeData("servers/mockedIP_0001")).thenReturn(ServerDataGsonFactory.toJson(data));
        serverService.clearJobPausedStatus();
        verify(jobNodeStorage).getJobNodeData("servers/mockedIP_0001");
        data.setPausedAndRemoveMark(false);
        verify(jobNodeStorage).updateJobNode("servers/mockedIP_0001", ServerDataGsonFactory.toJson(data));
    }
    
    @Test
    public void assertIsJobPausedManually() {
        JobRegistry.getInstance().addJobServerName("test_job", "mockedIP_0001");
        ServerData data = new ServerData("mockedHostName", "mockedIP", false);
        data.setPausedAndMark(true);
        when(jobNodeStorage.getJobNodeData("servers/mockedIP_0001")).thenReturn(ServerDataGsonFactory.toJson(data));
        assertTrue(serverService.isJobPausedManually());
        verify(jobNodeStorage).getJobNodeData("servers/mockedIP_0001");
    }
    
    @Test
    public void assertProcessServerShutdown() {
        JobRegistry.getInstance().addJobServerName("test_job", "mockedIP_0001");
        ServerData data = new ServerData("mockedHostName", "mockedIP", false);
        data.setShutdownAndMark(true);
        when(jobNodeStorage.getJobNodeData("servers/mockedIP_0001")).thenReturn(ServerDataGsonFactory.toJson(data));
        serverService.processServerShutdown();
        verify(jobNodeStorage).getJobNodeData("servers/mockedIP_0001");
        data.setShutdownAndRemoveMark(false);
        verify(jobNodeStorage).updateJobNode("servers/mockedIP_0001", ServerDataGsonFactory.toJson(data));
    }
    
    @Test
    public void assertUpdateServerStatus() {
        JobRegistry.getInstance().addJobServerName("test_job", "mockedIP_0001");
        ServerData data = new ServerData("mockedHostName", "mockedIP", false);
        when(jobNodeStorage.getJobNodeData("servers/mockedIP_0001")).thenReturn(ServerDataGsonFactory.toJson(data));
        serverService.updateServerStatus(ServerStatus.RUNNING);
        verify(jobNodeStorage).getJobNodeData("servers/mockedIP_0001");
        data.setStatus(ServerStatus.RUNNING);
        verify(jobNodeStorage).updateJobNode("servers/mockedIP_0001", ServerDataGsonFactory.toJson(data));
    }
    
    @Test
    public void assertRemoveServerStatus() {
        JobRegistry.getInstance().addJobServerName("test_job", "mockedIP_0001");
        serverService.removeServerStatus();
        verify(jobNodeStorage).removeJobNodeIfExisted("servers/mockedIP_0001");
    }
    
    @Test
    public void assertGetAllServers() {
        when(jobNodeStorage.getJobNodeChildrenKeys("servers")).thenReturn(Arrays.asList("host0_001", "host0_002", "host1_001", "host3_001"));
        assertThat(serverService.getAllServers(), is(Arrays.asList("host0_001", "host0_002", "host1_001", "host3_001")));
        verify(jobNodeStorage).getJobNodeChildrenKeys("servers");
    }
    
    @Test
    public void assertGetAvailableShardingServers() {
        when(jobNodeStorage.getJobNodeChildrenKeys("servers")).thenReturn(Arrays.asList("host0_001", "host0_002", "host1_001", "host3_001", "host2_001"));
        ServerData data0_0 = new ServerData("host0", "host0", false);
        data0_0.setDisabledAndMark(true);
        ServerData data0_1 = new ServerData("host0", "host0", false);
        ServerData data1 = new ServerData("host0", "host1", false);
        ServerData data2 = new ServerData("host0", "host2", false);
        data2.setShutdownAndMark(true);
        when(jobNodeStorage.getJobNodeData("servers/host0_001")).thenReturn(ServerDataGsonFactory.toJson(data0_0));
        when(jobNodeStorage.getJobNodeData("servers/host0_002")).thenReturn(ServerDataGsonFactory.toJson(data0_1));
        when(jobNodeStorage.getJobNodeData("servers/host1_001")).thenReturn(ServerDataGsonFactory.toJson(data1));
        when(jobNodeStorage.getJobNodeData("servers/host2_001")).thenReturn(ServerDataGsonFactory.toJson(data2));
        when(jobNodeStorage.getJobNodeData("servers/host3_001")).thenReturn(null);
        assertThat(serverService.getAvailableShardingServers(), is(Arrays.asList("host0_002", "host1_001")));
        verify(jobNodeStorage).getJobNodeChildrenKeys("servers");
        verify(jobNodeStorage).getJobNodeData("servers/host0_001");
        verify(jobNodeStorage).getJobNodeData("servers/host0_002");
        verify(jobNodeStorage).getJobNodeData("servers/host1_001");
        verify(jobNodeStorage).getJobNodeData("servers/host2_001");
        verify(jobNodeStorage).getJobNodeData("servers/host3_001");
    }
    
    @Test
    public void assertGetAvailableServers() {
        when(jobNodeStorage.getJobNodeChildrenKeys("servers")).thenReturn(Arrays.asList("host0_001", "host0_002", "host1_001", "host3_001", "host2_001", "host4_001"));
        ServerData data0_0 = new ServerData("host0", "host0", false);
        data0_0.setDisabledAndMark(true);
        ServerData data0_1 = new ServerData("host0", "host0", false);
        ServerData data1 = new ServerData("host0", "host1", false);
        ServerData data2 = new ServerData("host0", "host2", false);
        data2.setShutdownAndMark(true);
        ServerData data4 = new ServerData("host0", "host4", false);
        data4.setPausedAndMark(true);
        when(jobNodeStorage.getJobNodeData("servers/host0_001")).thenReturn(ServerDataGsonFactory.toJson(data0_0));
        when(jobNodeStorage.getJobNodeData("servers/host0_002")).thenReturn(ServerDataGsonFactory.toJson(data0_1));
        when(jobNodeStorage.getJobNodeData("servers/host1_001")).thenReturn(ServerDataGsonFactory.toJson(data1));
        when(jobNodeStorage.getJobNodeData("servers/host2_001")).thenReturn(ServerDataGsonFactory.toJson(data2));
        when(jobNodeStorage.getJobNodeData("servers/host3_001")).thenReturn(null);
        assertThat(serverService.getAvailableServers(), is(Arrays.asList("host0_002", "host1_001")));
        verify(jobNodeStorage).getJobNodeChildrenKeys("servers");
        verify(jobNodeStorage).getJobNodeData("servers/host0_001");
        verify(jobNodeStorage).getJobNodeData("servers/host0_002");
        verify(jobNodeStorage).getJobNodeData("servers/host1_001");
        verify(jobNodeStorage).getJobNodeData("servers/host2_001");
        verify(jobNodeStorage).getJobNodeData("servers/host3_001");
    }
    
    @Test
    public void assertIsAvailableServer() {
        JobRegistry.getInstance().addJobServerName("test_job", "host0_0001");
        ServerData data = new ServerData("host0", "host0", false);
        when(jobNodeStorage.getJobNodeData("servers/host0_0001")).thenReturn(ServerDataGsonFactory.toJson(data));
        assertTrue(serverService.isAvailableServer());
        verify(jobNodeStorage).getJobNodeData("servers/host0_0001");
    }
    
    @Test
    public void assertIsAvailableServerForOther() {
        ServerData data = new ServerData("host0", "host0", false);
        when(jobNodeStorage.getJobNodeData("servers/host0_0001")).thenReturn(ServerDataGsonFactory.toJson(data));
        assertTrue(serverService.isAvailableServer("host0_0001"));
        verify(jobNodeStorage).getJobNodeData("servers/host0_0001");
    }
    
    @Test
    public void assertIsLocalhostServerReadyWhenServerCrashed() {
        JobRegistry.getInstance().addJobServerName("test_job", "host0_0001");
        when(jobNodeStorage.getJobNodeData("servers/host0_0001")).thenReturn(null);
        assertFalse(serverService.isLocalhostServerReady());
        verify(jobNodeStorage).getJobNodeData("servers/host0_0001");
    }
    
    @Test
    public void assertIsLocalhostServerReadyWhenServerPaused() {
        JobRegistry.getInstance().addJobServerName("test_job", "host0_0001");
        ServerData data = new ServerData("host0", "host0", false);
        data.setPausedAndMark(true);
        when(jobNodeStorage.getJobNodeData("servers/host0_0001")).thenReturn(ServerDataGsonFactory.toJson(data));
        assertFalse(serverService.isLocalhostServerReady());
        verify(jobNodeStorage).getJobNodeData("servers/host0_0001");
    }
    
    @Test
    public void assertIsLocalhostServerReadyWhenServerDisabled() {
        JobRegistry.getInstance().addJobServerName("test_job", "host0_0001");
        ServerData data = new ServerData("host0", "host0", false);
        data.setDisabledAndMark(true);
        when(jobNodeStorage.getJobNodeData("servers/host0_0001")).thenReturn(ServerDataGsonFactory.toJson(data));
        assertFalse(serverService.isLocalhostServerReady());
        verify(jobNodeStorage).getJobNodeData("servers/host0_0001");
    }
    
    @Test
    public void assertIsLocalhostServerReadyWhenServerShutdown() {
        JobRegistry.getInstance().addJobServerName("test_job", "host0_0001");
        ServerData data = new ServerData("host0", "host0", false);
        data.setShutdownAndMark(true);
        when(jobNodeStorage.getJobNodeData("servers/host0_0001")).thenReturn(ServerDataGsonFactory.toJson(data));
        assertFalse(serverService.isLocalhostServerReady());
        verify(jobNodeStorage).getJobNodeData("servers/host0_0001");
    }
    
    @Test
    public void assertIsLocalhostServerReadyWhenServerRunning() {
        JobRegistry.getInstance().addJobServerName("test_job", "host0_0001");
        ServerData data = new ServerData("host0", "host0", false);
        data.setStatus(ServerStatus.RUNNING);
        when(jobNodeStorage.getJobNodeData("servers/host0_0001")).thenReturn(ServerDataGsonFactory.toJson(data));
        assertFalse(serverService.isLocalhostServerReady());
        verify(jobNodeStorage).getJobNodeData("servers/host0_0001");
    }
    
    @Test
    public void assertIsLocalhostServerReadyWhenServerReady() {
        JobRegistry.getInstance().addJobServerName("test_job", "host0_0001");
        ServerData data = new ServerData("host0", "host0", false);
        when(jobNodeStorage.getJobNodeData("servers/host0_0001")).thenReturn(ServerDataGsonFactory.toJson(data));
        assertTrue(serverService.isLocalhostServerReady());
        verify(jobNodeStorage).getJobNodeData("servers/host0_0001");
    }
    
    @Test
    public void assertIsLocalhostServerEnabled() {
        JobRegistry.getInstance().addJobServerName("test_job", "host0_0001");
        ServerData data = new ServerData("host0", "host0", false);
        when(jobNodeStorage.getJobNodeData("servers/host0_0001")).thenReturn(ServerDataGsonFactory.toJson(data));
        assertTrue(serverService.isLocalhostServerEnabled());
        verify(jobNodeStorage).getJobNodeData("servers/host0_0001");
    }
}
