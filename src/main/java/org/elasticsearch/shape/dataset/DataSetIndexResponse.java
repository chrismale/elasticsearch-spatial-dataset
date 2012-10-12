package org.elasticsearch.shape.dataset;

/**
 * Response from indexing a {@link ShapeDataSet}
 */
public class DataSetIndexResponse {

    private final int totalCount;

    /**
     * Creates a new DataSetIndexResponse
     *
     * @param totalCount Number of shapes indexed from the dataset
     */
    public DataSetIndexResponse(int totalCount) {
        this.totalCount = totalCount;
    }

    /**
     * @return Number of shapes indexed from the dataset
     */
    public int totalCount() {
        return totalCount;
    }
}
