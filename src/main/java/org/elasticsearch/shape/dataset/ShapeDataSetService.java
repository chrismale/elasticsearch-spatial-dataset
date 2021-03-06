package org.elasticsearch.shape.dataset;

import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.common.collect.Lists.newArrayList;
import static org.elasticsearch.common.collect.Maps.newHashMap;

/**
 * Service that facilitates indexing the Shapes from {@link ShapeDataSet}s
 */
public class ShapeDataSetService extends AbstractComponent {

    private final Client client;
    private final ThreadPool threadPool;

    private final List<ShapeDataSet> dataSets = newArrayList();
    private final Map<String, ShapeDataSet> dataSetsById = newHashMap();

    @Inject
    public ShapeDataSetService(Client client, Settings settings, ThreadPool threadPool) {
        super(settings);
        this.client = client;
        this.threadPool = threadPool;

        register(RemoteESRIShapeDataSet.NATURAL_EARTH_DATA_COUNTRIES);
    }

    /**
     * Registers the given {@link ShapeDataSet} with this service
     *
     * @param dataSet DataSet to register
     */
    public void register(ShapeDataSet dataSet) {
        dataSets.add(dataSet);
        dataSetsById.put(dataSet.id(), dataSet);
    }

    /**
     * @return Returns an unmodifiable view of the current registered ShapeDataSets
     */
    public List<ShapeDataSet> dataSets() {
        return Collections.unmodifiableList(dataSets);
    }

    /**
     * Returns the ShapeDataSet with the given ID
     *
     * @param id ID of the ShapeDataSet to retrieve
     * @return ShapeDataSet with the ID, or {@code null} if no ShapeDataSet with the ID
     *         is currently registered
     */
    public ShapeDataSet dataSet(String id) {
        return dataSetsById.get(id);
    }

    /**
     * Indexes the data from the given ShapeDataSet, into the given index and type
     *
     * @param dataSet ShapeDataSet whose data will be indexed
     * @param index Name of the index where the data will be indexed
     * @param type Name of the index type where the data will be indexed
     * @param batchSize The maximum batch size for indexing requests
     * @param listener Listener for success and failure of the indexing
     */
    public void index(
            final ShapeDataSet dataSet,
            final String index,
            final String type,
            final int batchSize,
            final ActionListener<DataSetIndexResponse> listener) {
        threadPool.executor(ThreadPool.Names.GENERIC).execute(new Runnable() {

            @Override
            public void run() {
                try {
                    int totalCount = index(dataSet, index, type, batchSize);
                    listener.onResponse(new DataSetIndexResponse(totalCount));
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }
        });
    }

    private int index(ShapeDataSet dataSet, String index, String type, int batchSize) throws IOException {
        Iterator<ShapeData> shapeDataIterator = dataSet.shapeData();

        int batchCount = 0;
        int totalCount = 0;

        Date insertDate = new Date();

        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

        while (shapeDataIterator.hasNext()) {
            ShapeData shapeData = shapeDataIterator.next();

            XContentBuilder contentBuilder = XContentFactory.jsonBuilder().startObject();
            for (Map.Entry<String, Object> entry : shapeData.data().entrySet()) {
                contentBuilder.field(entry.getKey(), entry.getValue());
            }

            contentBuilder.startObject(Fields.SHAPE);
            // TODO: Serialize Shape
            contentBuilder.endObject();

            contentBuilder.startObject(Fields.METADATA)
                    .field(Fields.DATASET_ID, dataSet.id())
                    .field(Fields.INSERT_DATE, insertDate);
            dataSet.addMetadata(contentBuilder);
            contentBuilder.endObject();

            bulkRequestBuilder.add(client.prepareIndex(index, type, shapeData.name()).setSource(contentBuilder).request());

            if (++batchCount == batchSize) {
                executeBulkRequest(bulkRequestBuilder);
                bulkRequestBuilder = client.prepareBulk();
                totalCount += batchCount;
                batchCount = 0;
            }
        }

        if (batchCount > 0) {
            executeBulkRequest(bulkRequestBuilder);
            totalCount += batchCount;
        }

        return totalCount;
    }

    private void executeBulkRequest(BulkRequestBuilder builder) {
        BulkResponse response = builder.execute().actionGet();
        if (response.hasFailures()) {
            throw new ElasticSearchIllegalStateException(response.buildFailureMessage());
        }
    }

    private static interface Fields {
        public final String SHAPE = "shape";
        public final String METADATA = "metadata";
        public final String DATASET_ID = "data_set_id";
        public final String INSERT_DATE = "insert_date";
    }
}
