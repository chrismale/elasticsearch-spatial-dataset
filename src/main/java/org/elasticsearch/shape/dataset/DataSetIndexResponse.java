package org.elasticsearch.shape.dataset;

public class DataSetIndexResponse {

    private final int totalCount;

    public DataSetIndexResponse(int totalCount) {
        this.totalCount = totalCount;
    }

    public int totalCount() {
        return totalCount;
    }
}
