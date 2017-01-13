/*
 * Copyright 1999-2015 dangdang.com. <p> Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License. </p>
 */

package com.dangdang.ddframe.job.lite.internal.server;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * Elastic Job服务器节点下的运行时数据.
 * 
 * @author liuzhifei
 * 
 */
@Setter
@Getter
@NoArgsConstructor
@ToString
public class ServerData {
    
    private static final String TRIGGER = "trigger";
    
    private static final String DISABLED = "disabled";
    
    private static final String PAUSED = "paused";
    
    private static final String SHUTDOWN = "shutdown";

    private String hostName;

    private String hostIP;

    private boolean trigger;

    private boolean paused;

    private boolean disabled;

    private boolean shutdown;

    private ServerStatus status = ServerStatus.READY;

    private String sharding;

    private String changedItem;

    public ServerData(String hostName, String hostIP, boolean disabled) {
        this.hostName = hostName;
        this.hostIP = hostIP;
        this.disabled = disabled;
    }

    public void removeTriggeredMark() {
        this.trigger = false;
        this.changedItem = null;
    }

    public void markTriggered() {
        this.trigger = true;
        this.changedItem = TRIGGER;
    }

    public void removePausedMark() {
        this.paused = false;
        this.changedItem = null;
    }

    public void markPaused() {
        this.paused = true;
        this.changedItem = PAUSED;
    }
    
    public void markResumed() {
        this.paused = false;
        this.changedItem = PAUSED;
    }

    public void markDisabled() {
        this.disabled = true;
        this.changedItem = DISABLED;
    }
    
    public void markEnabled() {
        this.disabled = false;
        this.changedItem = DISABLED;
    }

    public void removeShutdownMark() {
        this.shutdown = false;
        this.changedItem = null;
    }

    public void markShutdown() {
        this.shutdown = true;
        this.changedItem = SHUTDOWN;
    }

    public boolean isTriggeredWithMark() {
        return TRIGGER.equals(changedItem) && trigger;
    }

    public boolean isPausedWithMark() {
        return PAUSED.equals(changedItem) && paused;
    }

    public boolean isResumedWithMark() {
        return PAUSED.equals(changedItem) && !paused;
    }

    public boolean isDisabledWithMark() {
        return DISABLED.equals(changedItem) && disabled;
    }

    public boolean isEnabledWithMark() {
        return DISABLED.equals(changedItem) && !disabled;
    }

    public boolean isShutdownWithMark() {
        return SHUTDOWN.equals(changedItem) && shutdown;
    }

}
