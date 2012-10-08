package org.elasticsearch.plugin.shape.dataset;

import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;
import org.elasticsearch.shape.dataset.*;

import java.util.Collection;

import static org.elasticsearch.common.collect.Lists.newArrayList;

public class SpatialDataSetPlugin extends AbstractPlugin {

    @Override
    public Collection<Class<? extends Module>> modules() {
        Collection<Class<? extends Module>> modules = newArrayList();
        modules.add(SpatialDataSetModule.class);
        return modules;
    }

    public void onModule(RestModule module) {
        module.addRestAction(RestShapeDataSetListAction.class);
        module.addRestAction(RestShapeDataSetIndexAction.class);
    }

    @Override
    public String name() {
        return "spatial-dataset-plugin";
    }

    @Override
    public String description() {
        return "Plugin for indexing Spatial datasets";
    }
}
