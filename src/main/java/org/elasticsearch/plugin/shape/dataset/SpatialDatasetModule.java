package org.elasticsearch.plugin.shape.dataset;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.shape.dataset.*;

public class SpatialDataSetModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(ShapeDataSetService.class).asEagerSingleton();
    }
}
