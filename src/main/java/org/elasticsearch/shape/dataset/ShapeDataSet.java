package org.elasticsearch.shape.dataset;

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Iterator;

public interface ShapeDataSet {

    String id();

    Iterator<ShapeData> shapeData() throws IOException;

    void addMetadata(XContentBuilder contentBuilder) throws IOException;
}
