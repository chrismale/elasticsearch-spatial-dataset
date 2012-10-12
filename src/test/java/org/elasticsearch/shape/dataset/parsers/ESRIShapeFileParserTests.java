package org.elasticsearch.shape.dataset.parsers;

import com.spatial4j.core.shape.Shape;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;

/**
 * Tests for {@link ESRIShapeFileParser}
 */
public class ESRIShapeFileParserTests {

    @Test
    public void testReadFiles() throws IOException {
        FileInputStream fileInputStream = new FileInputStream(
                new File(getClass().getResource("/esri/test.shp").getFile()));
        ByteBuffer shpBuffer = ByteBuffer.allocate(2048000);
        fileInputStream.getChannel().read(shpBuffer);
        shpBuffer.flip();

        InputStream dbfInputStream = new FileInputStream(
                new File(getClass().getResource("/esri/test.dbf").getFile()));

        List<Shape> shapes = ESRIShapeFileParser.parseShpFile(shpBuffer);
        List<Map<String, Object>> shapeMetadata = ESRIShapeFileParser.parseDBFFile(dbfInputStream);

        assertEquals(shapes.size(), 177);
        assertEquals(shapeMetadata.size(), 177);

        dbfInputStream.close();
        fileInputStream.close();
    }
}
