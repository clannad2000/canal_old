package com.alibaba.otter.canal.client.adapter.es.core.service;

import com.alibaba.otter.canal.client.adapter.es.core.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.core.config.SchemaItem;
import com.alibaba.otter.canal.client.adapter.es.core.support.ESTemplate;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.Util;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @Description
 * @Author 黄念
 * @Date 2019/11/26
 * @Version1.0
 */
public interface SyncService {

    /**
     * 插入操作dml
     *
     * @param config es配置
     * @param dml    dml数据
     */
    void insert(ESSyncConfig config, Dml dml);

    /**
     * 更新操作dml
     *
     * @param config es配置
     * @param dml    dml数据
     */
    void update(ESSyncConfig config, Dml dml);

    /**
     * 删除操作dml
     *
     * @param config es配置
     * @param dml    dml数据
     */
    void delete(ESSyncConfig config, Dml dml);
}
