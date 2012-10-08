package org.elasticsearch.shape.dataset;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;

import java.io.IOException;

import static org.elasticsearch.rest.action.support.RestXContentBuilder.restContentBuilder;

public class RestShapeDataSetIndexAction extends BaseRestHandler {

    private final ShapeDataSetService dataSetService;

    @Inject
    public RestShapeDataSetIndexAction(Settings settings, Client client, RestController restController, ShapeDataSetService dataSetService) {
        super(settings, client);
        this.dataSetService = dataSetService;
        restController.registerHandler(RestRequest.Method.PUT, "/_shapedataset/index", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        try {
            String dataSetId = request.param(Fields.DATASET_ID);
            ShapeDataSet dataSet = dataSetService.dataSet(dataSetId);

            if (dataSet == null) {
                XContentBuilder builder = restContentBuilder(request)
                        .startObject()
                        .field(Fields.RESULT, "ShapeDataSet with ID [" + dataSetId + "] not found")
                        .endObject();
                channel.sendResponse(new XContentRestResponse(request, RestStatus.NOT_FOUND, builder));
            }

            String type = request.param(Fields.TYPE);
            if (type == null) {
                XContentBuilder builder = restContentBuilder(request)
                        .startObject()
                        .field(Fields.RESULT, "type missing")
                        .endObject();
                channel.sendResponse(new XContentRestResponse(request, RestStatus.BAD_REQUEST, builder));
            }

            String index = request.param(Fields.INDEX, Defaults.INDEX);
            int batchSize = request.paramAsInt(Fields.BATCH_SIZE, Defaults.BATCH_SIZE);

            dataSetService.index(dataSet, index, type, batchSize, new ActionListener<DataSetIndexResponse>() {

                @Override
                public void onResponse(DataSetIndexResponse dataSetIndexResponse) {
                    try {
                    XContentBuilder builder = restContentBuilder(request)
                            .startObject()
                            .field(Fields.RESULT, dataSetIndexResponse.totalCount() + " shapes indexed")
                            .endObject();
                    channel.sendResponse(new XContentRestResponse(request, RestStatus.OK, builder));
                    } catch (IOException ioe) {
                        onFailure(ioe);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    RestShapeDataSetIndexAction.this.onFailure(e, request, channel);
                }
            });

        } catch (IOException ioe) {
            onFailure(ioe, request, channel);
        }
    }

    private void onFailure(Throwable e, RestRequest request, RestChannel channel) {
        try {
            channel.sendResponse(new XContentThrowableRestResponse(request, e));
        } catch (IOException ioe) {
            logger.error("Failed to send error", ioe);
        }
    }

    private static interface Fields {
        String RESULT = "result";
        String DATASET_ID = "data_set_id";
        String INDEX = "index";
        String TYPE = "type";
        String BATCH_SIZE = "batch_size";
    }

    private static interface Defaults {
        String INDEX = "shapes";
        int BATCH_SIZE = Integer.MAX_VALUE;
    }
}
