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

import org.junit.Test;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.dangdang.ddframe.job.lite.internal.schedule.JobRegistry;

public final class ServerNodeTest {
    
    private ServerNode serverNode = new ServerNode("test_job");
    
    @BeforeClass
    public static void before(){
        JobRegistry.getInstance().addJobServerName("test_job", "host0_0001");
    }
    
    @AfterClass
    public static void after(){
        JobRegistry.getInstance().removeJobServerName("test_job");
    }
    
    @Test
    public void assertGetServerBaseNode() {
        assertThat(ServerNode.getServerBaseNode("host0"), is("servers/host0_"));
    }
    
    @Test
    public void assertGetServerNode() {
        assertThat(ServerNode.getServerNode("host0_0001"), is("servers/host0_0001"));
    }
    
    @Test
    public void assertGetServerName() {
        assertThat(ServerNode.getServerName("servers/host0_0001"), is("host0_0001"));
    }
    
    @Test
    public void assertIsLocalJobPath() {
        assertTrue(serverNode.isLocalJobPath("/test_job/servers/host0_0001"));
        assertFalse(serverNode.isLocalJobPath("/test_job/servers/host0_0002"));
        assertFalse(serverNode.isLocalJobPath("/other_job/servers/host0_0001"));
    }
    
    @Test
    public void assertIsServerJobPath() {
        assertTrue(serverNode.isServerJobPath("/test_job/servers/host0_0001"));
        assertFalse(serverNode.isServerJobPath("/test_job/servers"));
        assertFalse(serverNode.isServerJobPath("/other_job/servers/host0_0001"));
    }
    
}
