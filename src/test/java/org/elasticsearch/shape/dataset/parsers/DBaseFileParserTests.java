package org.elasticsearch.shape.dataset.parsers;

import org.elasticsearch.shape.dataset.ByteStreams;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;

import static org.testng.Assert.assertEquals;

/**
 * Tests for {@link DBaseFileParser}
 */
public class DBaseFileParserTests {

    @Test
    public void testDBaseFileParser() throws IOException {
        InputStream inputStream = new FileInputStream(new File(getClass().getResource("/esri/test.dbf").getFile()));
        byte[] contents = ByteStreams.toByteArray(inputStream);
        inputStream.close();

        DBaseFileParser parser = new DBaseFileParser(ByteBuffer.wrap(contents));
        assertEquals(parser.fields().size(), 29);

        Iterator<Object[]> records = parser.records();
        int count = 0;
        while (records.hasNext()) {
            Object[] values = records.next();
            assertEquals(values.length, 29);
            count++;
        }

        assertEquals(count, 177);
    }
}
