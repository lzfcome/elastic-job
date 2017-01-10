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

import java.io.IOException;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import com.dangdang.ddframe.job.util.json.GsonFactory;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

/**
 * Server节点数据的Gson工厂.
 *
 * @author liuzhifei
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ServerDataGsonFactory {

    static {
        GsonFactory.registerTypeAdapter(ServerData.class, new ServerDataGsonTypeAdapter());
    }

    /**
     * 将作业配置转换为JSON字符串.
     * 
     * @param liteJobConfig 作业配置对象
     * @return 作业配置JSON字符串
     */
    public static String toJson(final ServerData data) {
        return GsonFactory.getGson().toJson(data);
    }

    /**
     * 将JSON字符串转换为作业配置.
     *
     * @param liteJobConfigJson 作业配置JSON字符串
     * @return 作业配置对象
     */
    public static ServerData fromJson(final String data) {
        return GsonFactory.getGson().fromJson(data, ServerData.class);
    }

    /**
     * Lite作业配置的Json转换适配器.
     *
     * @author zhangliang
     */
    static final class ServerDataGsonTypeAdapter extends TypeAdapter<ServerData> {

        @Override
        public void write(JsonWriter out, ServerData value) throws IOException {
            out.beginObject();
            out.name("hostName").value(value.getHostName());
            out.name("hostIP").value(value.getHostIP());
            out.name("status").value(value.getStatus().name());
            out.name("sharding").value(value.getSharding());
            out.name("disabled").value(value.isDisabled());
            out.name("paused").value(value.isPaused());
            out.name("shutdown").value(value.isShutdown());
            out.name("trigger").value(value.isTrigger());
            out.name("changedItem").value(value.getChangedItem());
            out.endObject();

        }

        @Override
        public ServerData read(JsonReader in) throws IOException {
            ServerData data = new ServerData();
            in.beginObject();
            while (in.hasNext()) {
                String jsonName = in.nextName();
                switch (jsonName) {
                    case "hostName":
                        data.setHostName(in.nextString());
                        break;
                    case "hostIP":
                        data.setHostIP(in.nextString());
                        break;
                    case "status":
                        data.setStatus(ServerStatus.valueOf(in.nextString()));
                        break;
                    case "disabled":
                        data.setDisabled(in.nextBoolean());
                        break;
                    case "paused":
                        data.setPaused(in.nextBoolean());
                        break;
                    case "shutdown":
                        data.setShutdown(in.nextBoolean());
                        break;
                    case "trigger":
                        data.setTrigger(in.nextBoolean());
                        break;
                    case "sharding":
                        data.setSharding(in.nextString());
                        break;
                    case "changedItem":
                        data.setChangedItem(in.nextString());
                        break;
                    default:
                        break;
                }
            }
            in.endObject();
            return data;
        }
    }
}
