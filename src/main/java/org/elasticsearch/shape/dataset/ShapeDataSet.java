package org.elasticsearch.shape.dataset;

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Iterator;

/**
 * Abstraction of a source of Shapes that can be indexed
 */
public interface ShapeDataSet {

    /**
     * @return ID of the DataSet
     */
    String id();

    /**
     * Returns a new Iterator to retrieve the {@link ShapeData} contained in the set
     *
     * @return Iterator for retrieving the data from the set
     * @throws IOException Can be thrown by implementations when they encounter an IO problem
     */
    Iterator<ShapeData> shapeData() throws IOException;

    /**
     * Adds metadata to
     * @param contentBuilder
     * @throws IOException
     */
    void addMetadata(XContentBuilder contentBuilder) throws IOException;
}
