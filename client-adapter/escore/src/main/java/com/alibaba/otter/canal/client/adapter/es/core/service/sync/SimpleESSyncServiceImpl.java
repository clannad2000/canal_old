package com.alibaba.otter.canal.client.adapter.es.core.service.sync;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.client.adapter.es.core.annotation.SyncImpl;
import com.alibaba.otter.canal.client.adapter.es.core.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.core.config.ESSyncConfig.ESMapping;
import com.alibaba.otter.canal.client.adapter.es.core.config.SchemaItem;
import com.alibaba.otter.canal.client.adapter.es.core.config.SchemaItem.FieldItem;
import com.alibaba.otter.canal.client.adapter.es.core.service.ESSyncService;
import com.alibaba.otter.canal.client.adapter.es.core.support.ESSyncUtil;
import com.alibaba.otter.canal.client.adapter.es.core.support.ESTemplate;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.Util;
import com.alibaba.otter.canal.common.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Description
 * @Author 黄念
 * @Date 2019/11/22
 * @Version1.0
 */
@SyncImpl("simple")
public class SimpleESSyncServiceImpl extends ESSyncService implements SyncService {
    private static Logger logger = LoggerFactory.getLogger(SimpleESSyncServiceImpl.class);

    public SimpleESSyncServiceImpl(ESTemplate esTemplate) {
        super(esTemplate);
    }

    /**
     * 插入操作dml
     *
     * @param config es配置
     * @param dml    dml数据
     */
    @Override
    public void insert(ESSyncConfig config, Dml dml) {
        List<Map<String, Object>> dataList = dml.getData();
        if (dataList == null || dataList.isEmpty()) {
            return;
        }
        for (Map<String, Object> data : dataList) {
            if (data == null || data.isEmpty()) {
                continue;
            }

            if (!findSpecialField(config.getEsMapping(), "nested").isEmpty()) {
                nestedFieldInsert(config, dml, data);
                continue;
            }

            singleTableSimpleFiledInsert(config, dml, data);
        }
    }

    @Override
    public void update(ESSyncConfig config, Dml dml) {
        List<Map<String, Object>> dataList = dml.getData();
        List<Map<String, Object>> oldList = dml.getOld();
        if (dataList == null || dataList.isEmpty() || oldList == null || oldList.isEmpty()) {
            return;
        }
        int i = 0;
        for (Map<String, Object> data : dataList) {
            Map<String, Object> old = oldList.get(i);
            if (data == null || data.isEmpty() || old == null || old.isEmpty()) {
                continue;
            }

            if (!findSpecialField(config.getEsMapping(), "nested").isEmpty()) {
                nestedFieldInsert(config, dml, data);
                continue;
            }

            singleTableSimpleFiledUpdate(config, dml, data, old);
        }
    }

    @Override
    public void delete(ESSyncConfig config, Dml dml) {
        List<Map<String, Object>> dataList = dml.getData();
        if (dataList == null || dataList.isEmpty()) {
            return;
        }
        SchemaItem schemaItem = config.getEsMapping().getSchemaItem();

        for (Map<String, Object> data : dataList) {
            if (data == null || data.isEmpty()) {
                continue;
            }

            ESMapping mapping = config.getEsMapping();

            FieldItem pkFieldItem = schemaItem.getIdFieldItem(mapping);
            Map<String, Object> esFieldData = new LinkedHashMap<>();
            Object pkVal = esTemplate.getESDataFromDmlData(mapping, data, esFieldData);
            //getSpecialFieldForDmlDate(mapping, data, null, esFieldData);
            if (logger.isTraceEnabled()) {
                logger.trace("Main table delete es index, destination:{}, table: {}, index: {}, pk: {}",
                        config.getDestination(),
                        dml.getTable(),
                        mapping.get_index(),
                        pkVal);
            }
            esFieldData.remove(pkFieldItem.getFieldName());
            esFieldData.keySet().forEach(key -> esFieldData.put(key, null));
            esTemplate.delete(mapping, pkVal, esFieldData);
        }
    }


    /**
     * 简单字段insert
     *
     * @param config es配置
     * @param dml    dml信息
     * @param data   单行dml数据
     */
    private void singleTableSimpleFiledInsert(ESSyncConfig config, Dml dml, Map<String, Object> data) {
        ESMapping mapping = config.getEsMapping();
        Map<String, Object> esFieldData = new LinkedHashMap<>();
        Object idVal = esTemplate.getESDataFromDmlData(mapping, data, esFieldData);
        getSpecialFieldForDmlDate(mapping, data, null, esFieldData);
        if (logger.isTraceEnabled()) {
            logger.trace("Single table insert to es index, destination:{}, table: {}, index: {}, id: {}",
                    config.getDestination(),
                    dml.getTable(),
                    mapping.get_index(),
                    idVal);
        }
        esTemplate.insert(mapping, idVal, esFieldData);
    }

    /**
     * 单表简单字段update
     *
     * @param config es配置
     * @param dml    dml信息
     * @param data   单行data数据
     * @param old    单行old数据
     */
    private void singleTableSimpleFiledUpdate(ESSyncConfig config, Dml dml, Map<String, Object> data,
                                              Map<String, Object> old) {
        ESMapping mapping = config.getEsMapping();
        Map<String, Object> esFieldData = new LinkedHashMap<>();
        Object idVal = esTemplate.getESDataFromDmlData(mapping, data, old, esFieldData);
        getSpecialFieldForDmlDate(mapping, data, null, esFieldData);
        if (logger.isTraceEnabled()) {
            logger.trace("Main table update to es index, destination:{}, table: {}, index: {}, id: {}",
                    config.getDestination(),
                    dml.getTable(),
                    mapping.get_index(),
                    idVal);
        }
        esTemplate.update(mapping, idVal, esFieldData);
    }

    public void getSpecialFieldForDmlDate(ESMapping mapping, Map<String, Object> dmlData, Map<String, Object> dmlOld,
                                          Map<String, Object> esFieldData) {
        mapping.getSpecialFields().forEach((key, value) -> {
            esFieldData.remove(key);
        });

//        if (dmlOld != null) {
//            Map<String, FieldItem> selectFields = mapping.getSchemaItem().getSelectFields();
//            dmlOld.forEach((oldKey, oldVal) -> {
//                mapping.getSpecialFields().forEach((key, value) -> {
//                    FieldItem fieldItem = selectFields.get(key);
//                    fieldItem.getColumnItems().forEach(columnItem -> {
//                        if (oldKey.equals(columnItem.getColumnName())) {
//                            getSpecialFieldValFromData(mapping, dmlData, esFieldData, key, value);
//                        }
//                    });
//                });
//            });
//            return;
//        }

        mapping.getSpecialFields().forEach((key, value) -> {
            getSpecialFieldValFromData(mapping, dmlData, esFieldData, key, value);
        });
    }

    private void getSpecialFieldValFromData(ESMapping esMapping, Map<String, Object> dmlData, Map<String, Object> esFieldData, String fieldName, ESMapping.FieldMapping value) {
        if (esFieldData.containsKey(fieldName)) {
            return;
        }
        Map<String, Object> map = new HashMap();
        switch (value.getType()) {
            case "object":
                //处理obj类型
                value.getProperties().forEach((k, val) -> {
                    map.put(val, dmlData.remove(k));
                });
                esFieldData.put(fieldName, map);
                dmlData.put(fieldName, map);
                break;
            case "array":
                //处理数组类型
                StringBuilder stringBuilder = new StringBuilder();
                for (String ele : Arrays.asList(value.getElement().split(","))) {
                    stringBuilder.append(dmlData.get(ele.trim()));
                }
                esFieldData.put(fieldName, stringBuilder.toString());
                dmlData.put(fieldName, stringBuilder.toString());
                break;
            case "geoPoint":
                //处理地理位置类型
                String[] arr = value.getElement().split(",");
                Map<String, Double> location = new HashMap<>();
                String lat = dmlData.get(arr[0].trim()) != null ? dmlData.remove(arr[0].trim()).toString() : "0";
                String lon = dmlData.get(arr[1].trim()) != null ? dmlData.remove(arr[1].trim()).toString() : "0";
                location.put("lat", Double.valueOf(lat));
                location.put("lon", Double.valueOf(lon));
                esFieldData.put(fieldName, location);
                dmlData.put(fieldName, location);
                break;
        }
    }

    public Map<String, ESMapping.FieldMapping> findSpecialField(ESMapping esMapping, String type) {
        Map<String, ESMapping.FieldMapping> map = new ConcurrentHashMap<>();
        esMapping.getSpecialFields().forEach((key, value) -> {
            if (value.getType().equalsIgnoreCase(type)) {
                map.put(key, value);
            }
        });
        return map;
    }


    /**
     * 嵌套字段insert
     *
     * @param config es配置
     * @param dml    dml信息
     * @param data   单行dml数据
     */
    private void nestedFieldInsert(ESSyncConfig config, Dml dml, Map<String, Object> data) {
        ESMapping mapping = config.getEsMapping();
        String sql = mapping.getSql();
        String condition = ESSyncUtil.pkConditionSql(mapping, data);
        sql = ESSyncUtil.appendCondition(sql, condition);
        DataSource ds = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
        if (logger.isTraceEnabled()) {
            logger.trace("Main table insert to es index by query sql, destination:{}, table: {}, index: {}, sql: {}",
                    config.getDestination(),
                    dml.getTable(),
                    mapping.get_index(),
                    sql.replace("\n", " "));
        }
        Util.sqlRS(ds, sql, rs -> {
            try {
                Map<String, Object> esFieldData = new LinkedHashMap<>();
                Map<String, ESMapping.FieldMapping> specialFieldList = findSpecialField(config.getEsMapping(), "nested");
                List<Map<String, Object>> queryDate = Util.getQueryDate(rs);
                specialFieldList.forEach((key, value) -> {
                    List<Map<String, Object>> fieldDate = Util.getFieldDate(queryDate, key, Arrays.asList(value.getElement().split(",")));
                    esFieldData.put(key, fieldDate);
                });

                String idFieldName = mapping.get_id() == null ? mapping.getPk() : mapping.get_id();
                Object idVal = queryDate.get(0).get(idFieldName);
                esFieldData.put(idFieldName, idVal);
                if (logger.isTraceEnabled()) {
                    logger.trace(
                            "Main table insert to es index by query sql, destination:{}, table: {}, index: {}, id: {}",
                            config.getDestination(),
                            dml.getTable(),
                            mapping.get_index(),
                            idVal);
                }
                esTemplate.insert(mapping, idVal, esFieldData);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return 0;
        });
    }

}
