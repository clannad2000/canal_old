package com.alibaba.otter.canal.client.adapter.es.core.factory;

import com.alibaba.otter.canal.client.adapter.es.core.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.core.service.ESSyncService;
import com.alibaba.otter.canal.client.adapter.es.core.service.SyncService;
import com.alibaba.otter.canal.client.adapter.es.core.support.ESTemplate;
import org.apache.commons.lang.StringUtils;

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

    private static Map<String, SyncService> serviceMap = new ConcurrentHashMap<>();

    public static synchronized SyncService getInstance(ESTemplate esTemplate, String name) throws Exception {
        if (serviceMap.get(name) != null) {
            return serviceMap.get(name);
        }
        String className = SyncService.class.getPackage().getName() + "." + StringUtils.capitalize(name) + "ESSyncServiceImpl";
        Class<?> cls = Class.forName(className);
        Constructor<?> constructor = cls.getConstructor(ESTemplate.class);
        SyncService syncService = (SyncService) constructor.newInstance(esTemplate);
        //SyncService syncService = (SyncService) cls.newInstance();
        serviceMap.put(name, syncService);
        return syncService;
    }
}
