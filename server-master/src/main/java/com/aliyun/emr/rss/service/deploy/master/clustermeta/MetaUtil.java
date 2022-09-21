/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.emr.rss.service.deploy.master.clustermeta;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.aliyun.emr.rss.common.meta.DiskInfo;
import com.aliyun.emr.rss.common.meta.WorkerInfo;
import com.aliyun.emr.rss.common.protocol.message.ControlMessages;
import com.aliyun.emr.rss.common.util.Utils;

public class MetaUtil {
  private MetaUtil() {}

  public static WorkerInfo addrToInfo(ResourceProtos.WorkerAddress address) {
    return new WorkerInfo(
        address.getHost(),
        address.getRpcPort(),
        address.getPushPort(),
        address.getFetchPort(),
        address.getReplicatePort());
  }

  public static ResourceProtos.WorkerAddress infoToAddr(WorkerInfo info) {
    return ResourceProtos.WorkerAddress.newBuilder()
        .setHost(info.host())
        .setRpcPort(info.rpcPort())
        .setPushPort(info.pushPort())
        .setFetchPort(info.fetchPort())
        .setReplicatePort(info.replicatePort())
        .build();
  }

  public static Map<String, DiskInfo> fromPbDiskInfos(
      Map<String, ResourceProtos.DiskInfo> diskInfos) {
    Map<String, DiskInfo> map = new HashMap<>();

    diskInfos.forEach(
        (k, v) -> {
          DiskInfo diskInfo =
              new DiskInfo(
                  v.getMountPoint(), v.getUsableSpace(), v.getAvgFlushTime(), v.getUsedSlots());
          diskInfo.setStatus(Utils.toDiskStatus(v.getStatus()));
          map.put(k, diskInfo);
        });
    return map;
  }

  public static Map<String, ResourceProtos.DiskInfo> toPbDiskInfos(
      Map<String, DiskInfo> diskInfos) {
    Map<String, ResourceProtos.DiskInfo> map = new HashMap<>();
    diskInfos.forEach(
        (k, v) ->
            map.put(
                k,
                ResourceProtos.DiskInfo.newBuilder()
                    .setMountPoint(v.mountPoint())
                    .setUsableSpace(v.actualUsableSpace())
                    .setAvgFlushTime(v.avgFlushTime())
                    .setUsedSlots(v.activeSlots())
                    .setStatus(v.status().getValue())
                    .build()));
    return map;
  }

  public static ConcurrentHashMap<ControlMessages.UserIdentifier, Long> fromPbUserUsages(
      Map<String, Long> userUsages) {
    ConcurrentHashMap<ControlMessages.UserIdentifier, Long> map = new ConcurrentHashMap<>();

    userUsages.forEach((k, v) -> map.put(ControlMessages.UserIdentifier$.MODULE$.fromString(k), v));
    return map;
  }

  public static Map<String, Long> toPbUserUsages(
      ConcurrentHashMap<ControlMessages.UserIdentifier, Long> userUsages) {
    Map<String, Long> map = new HashMap<>();
    userUsages.forEach((k, v) -> map.put(k.toString(), v));
    return map;
  }

  public static ControlMessages.UserIdentifier fromPbUserIdentifier(
      ResourceProtos.UserIdentifier userIdentifier) {
    return new ControlMessages.UserIdentifier(
        userIdentifier.getTenantId(), userIdentifier.getName());
  }

  public static ResourceProtos.UserIdentifier toPbUserIdentifier(
      ControlMessages.UserIdentifier userIdentifier) {
    return ResourceProtos.UserIdentifier.newBuilder()
        .setTenantId(userIdentifier.tenantId())
        .setName(userIdentifier.name())
        .build();
  }
}
