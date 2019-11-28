package com.alibaba.otter.canal.client.adapter.es.core.factory;

import com.alibaba.otter.canal.client.adapter.es.core.annotation.SyncImpl;
import com.alibaba.otter.canal.client.adapter.es.core.service.sync.SyncService;
import com.alibaba.otter.canal.client.adapter.es.core.support.ESTemplate;
import com.alibaba.otter.canal.client.adapter.es.core.util.Util;


import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Description
 * @Author 黄念
 * @Date 2019/11/23
 * @Version1.0
 */
public class SyncServiceFactory {
    private static ESTemplate esTemplate;
    private static Map<String, SyncService> serviceMap = new ConcurrentHashMap<>();

    public static void initSyncServiceImpl(ESTemplate esTemplate) throws Exception {
        SyncServiceFactory.esTemplate = esTemplate;
        for (Class<?> cls : Util.getClasses(SyncService.class.getPackage().getName())) {
            SyncImpl annotation = cls.getAnnotation(SyncImpl.class);
            if (annotation != null) {
                Constructor<?> constructor = cls.getConstructor(ESTemplate.class);
                SyncService syncService = (SyncService) constructor.newInstance(esTemplate);
                serviceMap.put(annotation.value(), syncService);
            }
        }
    }

    public static SyncService getInstance(String name) {
        return serviceMap.get(name);
    }
}
