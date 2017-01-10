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

    public void setTriggerAndRemoveMark(boolean trigger) {
        this.trigger = trigger;
        this.changedItem = null;
    }

    public void setTriggerAndMark(boolean trigger) {
        this.trigger = trigger;
        this.changedItem = ServerNode.TRIGGER_APPENDIX;
    }

    public void setPausedAndRemoveMark(boolean paused) {
        this.paused = paused;
        this.changedItem = null;
    }

    public void setPausedAndMark(boolean paused) {
        this.paused = paused;
        this.changedItem = ServerNode.PAUSED_APPENDIX;
    }

    public void setDisabledAndRemoveMark(boolean disabled) {
        this.disabled = disabled;
        this.changedItem = null;
    }

    public void setDisabledAndMark(boolean disabled) {
        this.disabled = disabled;
        this.changedItem = ServerNode.DISABLED_APPENDIX;
    }

    public void setShutdownAndRemoveMark(boolean shutdown) {
        this.shutdown = shutdown;
        this.changedItem = null;
    }

    public void setShutdownAndMark(boolean shutdown) {
        this.shutdown = shutdown;
        this.changedItem = ServerNode.SHUTDOWN_APPENDIX;
    }

    public boolean isTriggerWithMark() {
        return ServerNode.TRIGGER_APPENDIX.equals(changedItem) && trigger;
    }

    public boolean isPausedWithMark() {
        return ServerNode.PAUSED_APPENDIX.equals(changedItem) && paused;
    }

    public boolean isResumedWithMark() {
        return ServerNode.PAUSED_APPENDIX.equals(changedItem) && !paused;
    }

    public boolean isDisabledWithMark() {
        return ServerNode.DISABLED_APPENDIX.equals(changedItem) && disabled;
    }

    public boolean isEnabledWithMark() {
        return ServerNode.DISABLED_APPENDIX.equals(changedItem) && !disabled;
    }

    public boolean isShutdownWithMark() {
        return ServerNode.SHUTDOWN_APPENDIX.equals(changedItem) && shutdown;
    }

}
