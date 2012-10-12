package org.elasticsearch.shape.dataset;

import com.spatial4j.core.shape.Shape;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.common.io.Closeables;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.shape.dataset.parsers.ESRIShapeFileParser;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.elasticsearch.common.collect.Lists.newArrayList;

/**
 * {@link ShapeDataSet} implementation supporting remotely archieved ESRI Shapefile bundles
 * such as those used by http://www.naturalearthdata.com
 */
public class RemoteESRIShapeDataSet implements ShapeDataSet {

    public static final ShapeDataSet NATURAL_EARTH_DATA_COUNTRIES = new RemoteESRIShapeDataSet("natural_earth_data_cities",
            "http://www.naturalearthdata.com/http//www.naturalearthdata.com/download/110m/cultural/110m-admin-0-countries.zip",
            "NAME");

    private static final String SHP_SUFFIX = ".shp";
    private static final String DBF_SUFFIX = ".dbf";

    private final String id;
    private final URL url;
    private final String nameField;

    /**
     * Constructs a new RemoteESRIShapeDataSet which will retrieve from the given URL
     *
     * @param id ID for the DataSet
     * @param url URL to retrieve the shapefile data from
     * @param nameField Name of the metadata field that has the Shape names
     */
    public RemoteESRIShapeDataSet(String id, String url, String nameField) {
        this.id = id;
        this.nameField = nameField;
        try {
            // Construct it ourselves so that fields don't have to catch exception
            this.url = new URL(url);
        } catch (MalformedURLException mue) {
            throw new ElasticSearchIllegalArgumentException("Invalid URL for data set", mue);
        }
    }

    /**
     * {@inheritDoc}
     */
    public String id() {
        return id;
    }

    /**
     * {@inheritDoc}
     */
    public Iterator<ShapeData> shapeData() throws IOException {
        InputStream urlInputStream = null;
        ZipInputStream zipInputStream = null;

        try {
            urlInputStream = url.openStream();
            zipInputStream = new ZipInputStream(urlInputStream);

            List<Shape> shapes = null;
            List<Map<String, Object>> shapeMetadata = null;

            ZipEntry zipEntry;
            while ((zipEntry = zipInputStream.getNextEntry()) != null) {
                String name = zipEntry.getName();

                if (name.endsWith(SHP_SUFFIX)) {
                    shapes = ESRIShapeFileParser.parseShpFile(ByteBuffer.wrap(ByteStreams.toByteArray(zipInputStream)));
                } else if (name.endsWith(DBF_SUFFIX)) {
                    // For some reason javadbf fails when reading directly from the ZIPInputStream
                    // But works when read from a ByteArrayInputStream.
                    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(ByteStreams.toByteArray(zipInputStream));
                    shapeMetadata = ESRIShapeFileParser.parseDBFFile(byteArrayInputStream);
                }

                zipInputStream.closeEntry();
            }

            if (shapes == null) {
                throw new ElasticSearchIllegalStateException("Dataset does not contain SHP file");
            } else if (shapeMetadata == null) {
                throw new ElasticSearchIllegalStateException("Dataset does not contain DBF file");
            }

            List<ShapeData> shapeData = newArrayList();

            for (int i = 0; i < shapes.size(); i++) {
                Map<String, Object> metadata = shapeMetadata.get(i);
                String name = (String) metadata.remove(nameField);
                if (name == null) {
                    throw new ElasticSearchIllegalArgumentException("Could not find Shape name in field [" + nameField + "]");
                }

                shapeData.add(new ShapeData(shapes.get(i), name.trim(), metadata));
            }

            return shapeData.iterator();
        } finally {
            Closeables.closeQuietly(zipInputStream);
            Closeables.closeQuietly(urlInputStream);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void addMetadata(XContentBuilder contentBuilder) throws IOException {
        contentBuilder.field("source_url", url.toExternalForm());
    }
}
