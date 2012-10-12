package org.elasticsearch.shape.dataset;

import com.spatial4j.core.shape.Shape;

import java.util.Map;

/**
 * Wrapper for data about a Shape
 */
public class ShapeData {

    private final Shape shape;
    private final String name;
    private final Map<String, Object> data;

    /**
     * Creates a new ShapeData wrapping the given Shape with its name and metadata
     *
     * @param shape Shape the data is about
     * @param name Name of the Shape
     * @param data Metadata about the Shape
     */
    public ShapeData(Shape shape, String name, Map<String, Object> data) {
        this.shape = shape;
        this.name = name;
        this.data = data;
    }

    /**
     * @return Shape the data is about
     */
    public Shape shape() {
        return shape;
    }

    /**
     * @return Name of the Shape
     */
    public String name() {
        return name;
    }

    /**
     * @return Metadata about the Shape
     */
    public Map<String, Object> data() {
        return data;
    }
}
