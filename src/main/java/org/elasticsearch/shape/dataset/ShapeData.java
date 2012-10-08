package org.elasticsearch.shape.dataset;

import com.spatial4j.core.shape.Shape;

import java.util.Map;

public class ShapeData {

    private final Shape shape;
    private final String name;
    private final Map<String, Object> data;

    public ShapeData(Shape shape, String name, Map<String, Object> data) {
        this.shape = shape;
        this.name = name;
        this.data = data;
    }

    public Shape shape() {
        return shape;
    }

    public String name() {
        return name;
    }

    public Map<String, Object> data() {
        return data;
    }
}
