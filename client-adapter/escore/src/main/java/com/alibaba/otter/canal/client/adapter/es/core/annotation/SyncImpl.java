package com.alibaba.otter.canal.client.adapter.es.core.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * @Description
 * @Author 黄念
 * @Date 2019/11/27
 * @Version1.0
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface SyncImpl {
    public String value() default "default";
}
