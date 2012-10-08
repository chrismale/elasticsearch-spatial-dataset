package org.elasticsearch.shape.dataset;

import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Iterator;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class RemoteESRIShapeDataSetTests {

    @Test
    public void testShapeData() throws IOException {
        String filePath = RemoteESRIShapeDataSetTests.class.getResource(
                "/esri/test.zip").getFile();
        RemoteESRIShapeDataSet testDataSet = new RemoteESRIShapeDataSet(
                "test_data_set", "file://" + filePath, "NAME");

        Iterator<ShapeData> shapeData = testDataSet.shapeData();

        int totalCount = 0;
        while (shapeData.hasNext()) {
            assertNotNull(shapeData.next().shape());
            totalCount++;
        }

        assertEquals(totalCount, 177);
    }
}
