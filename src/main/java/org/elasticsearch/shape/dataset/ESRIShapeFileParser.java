package org.elasticsearch.shape.dataset;

import com.linuxense.javadbf.DBFField;
import com.linuxense.javadbf.DBFReader;
import com.spatial4j.core.exception.InvalidShapeException;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.impl.PointImpl;
import com.spatial4j.core.shape.jts.JtsGeometry;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.ElasticSearchParseException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Simple implementation of a parser of ESRI ShapeFiles.  Implementation is derived from
 * the technical description of the file formats provided in http://www.esri.com/library/whitepapers/pdfs/shapefile.pdf.
 * <p>
 * Supported Shape types are:
 * <ul>
 * <li>Polygon</li>
 * </ul>
 * </p>
 */
@SuppressWarnings("unused")
public class ESRIShapeFileParser {

    public static final Shape DUMMY_SHAPE = new PointImpl(0, 0, GeoShapeConstants.SPATIAL_CONTEXT);

    private static final int FILE_CODE = 9994;
    private static final int VERSION = 1000;

    /**
     * Enum of the ShapeTypes currently supported in this impl, along with their
     * codes as found in shp files.
     */
    private static enum ShapeType {

        POINT(1),
        POLYGON(5);

        private final int value;

        ShapeType(int value) {
            this.value = value;
        }

        /**
         * Gets the {@link ShapeType} which is represented with the given value in files
         *
         * @param value Value of the ShapeType to find
         * @return ShapeType with the given value
         * @throws ElasticSearchIllegalArgumentException Thrown if no ShapeType with the
         *         value exists
         */
        public static ShapeType getShapeTypeForValue(int value) {
            for (ShapeType shapeType : ShapeType.values()) {
                if (shapeType.value == value) {
                    return shapeType;
                }
            }
            throw new ElasticSearchParseException("Unknown ShapeType with value [" + value + "]");
        }
    }

    /**
     * Parses the SHP file, extracting the Shapes contained
     *
     * @param shpBuffer SHP file contents to parse
     * @return List of Shapes contained in the file
     */
    public static List<Shape> parseShpFile(ByteBuffer shpBuffer) {
        ShapeType shapeType = parseHeader(shpBuffer);

        List<Shape> shapes = new ArrayList<Shape>();

        while (shpBuffer.hasRemaining()) {
            shapes.add(parseRecord(shpBuffer, shapeType));
        }

        return shapes;
    }

    /**
     * Parses the SHP file header.  Note, only the type of Shapes contained in
     * the file is returned.  All other information is read, validated, and discarded.
     *
     * @param headerBuffer ByteBuffer containing the header
     * @return {@link ShapeType} representing the types of Shapes contained in the file
     */
    private static ShapeType parseHeader(ByteBuffer headerBuffer) {
        int fileCode = headerBuffer.getInt();
        if (fileCode != FILE_CODE) {
            throw new ElasticSearchParseException("Header does not have correct file code. " +
                    "Expected [" + FILE_CODE + "] but found [" + fileCode + "]");
        }

        // Unused blocks of data
        headerBuffer.getInt();
        headerBuffer.getInt();
        headerBuffer.getInt();
        headerBuffer.getInt();
        headerBuffer.getInt();

        // We don't need to keep track of this since our FileChannel knows the full
        // file size
        int fileLength = headerBuffer.getInt();

        headerBuffer.order(ByteOrder.LITTLE_ENDIAN);

        int version = headerBuffer.getInt();
        if (version != VERSION) {
            throw new ElasticSearchParseException("Header does not have correct version. " +
                    "Expected [" + VERSION + "] but found [" + version + "]");
        }

        ShapeType shapeType = ShapeType.getShapeTypeForValue(headerBuffer.getInt());

        // Maximum Bounding Rectangle (MBR) for all Shapes
        double minX = headerBuffer.getDouble();
        double minY = headerBuffer.getDouble();
        double maxX = headerBuffer.getDouble();
        double maxY = headerBuffer.getDouble();
        double minZ = headerBuffer.getDouble();
        double maxZ = headerBuffer.getDouble();
        double minM = headerBuffer.getDouble();
        double maxM = headerBuffer.getDouble();

        return shapeType;
    }

    /**
     * Parses the current Shape record
     *
     * @param recordBuffer ByteBuffer containing the record
     * @param shapeType Type of Shape that will be read
     * @return Shape read from the SHP File
     */
    private static Shape parseRecord(ByteBuffer recordBuffer, ShapeType shapeType) {
        // Record number is ignored, we assume the records are in order
        int recordNumber = recordBuffer.getInt();
        // Length is defined as 16-bit words in file
        int contentLength = recordBuffer.getInt();

        if (shapeType == ShapeType.POLYGON) {
            return parsePolygon(recordBuffer);
        } else {
            throw new UnsupportedOperationException("ShapeType [" + shapeType.name() + "] not currently supported");
        }
    }

    /**
     * Parses a Polygon shape from the contents of the given ByteBuffer
     *
     * @param polygonBuffer ByteBuffer holding the representation of a polygon
     * @return Parsed Polygon
     */
    private static Shape parsePolygon(ByteBuffer polygonBuffer) {
        polygonBuffer.order(ByteOrder.LITTLE_ENDIAN);

        int shapeType = polygonBuffer.getInt();
        if (shapeType != ShapeType.POLYGON.value) {
            throw new ElasticSearchParseException("Polygon record does not have correct ShapeType. " +
                    "Expected [" + ShapeType.POLYGON.value + "] but found [" + shapeType + "]");
        }

        double minX = polygonBuffer.getDouble();
        double minY = polygonBuffer.getDouble();
        double maxX = polygonBuffer.getDouble();
        double maxY = polygonBuffer.getDouble();

        int numParts = polygonBuffer.getInt();
        int numPoints = polygonBuffer.getInt();

        int[] parts = new int[numParts];

        for (int i = 0; i < numParts; i++) {
            parts[i] = polygonBuffer.getInt();
        }

        List<Coordinate> points = new ArrayList<Coordinate>();

        for (int i = 0; i < numPoints; i++) {
            Coordinate coordinate = parseCoordinates(polygonBuffer);
            points.add(coordinate);
        }

        List<LinearRing> rings = new ArrayList<LinearRing>(numParts);
        int lastPointer = points.size();

        for (int i = parts.length - 1; i >= 0; i--) {
            int pointer = parts[i];
            List<Coordinate> ringPoints = points.subList(pointer, lastPointer);

            // TODO Some Polygons (such as Antarctica) have crazy latitude and longitudes
            // we need to think about how best to normalize them (which OGR seems to do)
            for (Coordinate point : ringPoints) {
                if (!isValidCoordinate(point)) {
                    return DUMMY_SHAPE;
                }
            }
            rings.add(GeoShapeConstants.GEOMETRY_FACTORY.createLinearRing(ringPoints.toArray(new Coordinate[ringPoints.size()])));
            lastPointer = pointer;
        }

        // ShapeFiles do not differentiate between Polygons, Polygons with holes, MultiPolygons and MultiPolygons
        // with holes.  Consequently we need to infer which rings are part of a single Polygon.
        // This algorithm assumes that holes for a polygon follow its shell ring and are contained within.
        List<List<LinearRing>> polygons = new ArrayList<List<LinearRing>>();
        List<LinearRing> currentPolygon = new ArrayList<LinearRing>();
        LinearRing currentRing = rings.get(0);
        currentPolygon.add(currentRing);

        for (int i = 1; i < rings.size(); i++) {
            LinearRing ring = rings.get(i);
            if (currentRing.covers(ring)) {
                currentPolygon.add(currentRing);
            } else {
                polygons.add(currentPolygon);
                currentPolygon = new ArrayList<LinearRing>();
                currentPolygon.add(ring);
                currentRing = ring;
            }
        }

        polygons.add(currentPolygon);

        Polygon[] builtPolygons = toPolygons(polygons);

        // TODO Some Polygons (such as Canada) contain points that fail the validations in JtsGeometry
        // Theres not much we can do about this since it is likely that using the Shape will cause problems
        try {
            return builtPolygons.length == 1 ? new JtsGeometry(builtPolygons[0], GeoShapeConstants.SPATIAL_CONTEXT, true) :
                    new JtsGeometry(GeoShapeConstants.GEOMETRY_FACTORY.createMultiPolygon(builtPolygons), GeoShapeConstants.SPATIAL_CONTEXT, true);
        } catch (InvalidShapeException ise) {
            return DUMMY_SHAPE;
        }
    }

    /**
     * Converts the given List of LinearRings into Polygons.  It is assumed that the
     * first LinearRing in each list is the shell and subsequent rings are holes
     *
     * @param rings List of LinearRings to be converted into Polygons
     * @return Polygons constructed from the LinearRings
     */
    private static Polygon[] toPolygons(List<List<LinearRing>> rings) {
        Polygon[] polygons = new Polygon[rings.size()];

        for (int i = 0; i < rings.size(); i++) {
            List<LinearRing> polygon = rings.get(i);
            LinearRing shell = polygon.get(polygon.size() - 1);
            LinearRing[] holes = null;
            if (polygon.size() > 1) {
                holes = new LinearRing[polygon.size() - 1];
                for (int j = 0; i < holes.length; j++) {
                    holes[j] = polygon.get(j);
                }
            }
            polygons[i] = GeoShapeConstants.GEOMETRY_FACTORY.createPolygon(shell, holes);
        }
        return polygons;
    }

    /**
     * Parses coordinates X and Y from the given Buffer.  Note, in the technical
     * description these are referred to as Point datatypes however they do not
     * include the ShapeType value in the file therefore they are not proper Points,
     * instead they are just coordinates.
     *
     * @param coordinateBuffer Buffer containing the coordinates to read
     * @return Coordinate holding the X and Y values
     */
    private static Coordinate parseCoordinates(ByteBuffer coordinateBuffer) {
        coordinateBuffer.order(ByteOrder.LITTLE_ENDIAN);

        double x = coordinateBuffer.getDouble();
        double y = coordinateBuffer.getDouble();
        return new Coordinate(x, y);
    }

    /**
     * Validates that the given coordinate values fall within the typical ranges
     * of -180 <= lon <= 180 && -90 <= lat <= 90
     *
     * @param coordinate Coordinate to validate
     * @return {@code true} if the Coordinate has valid values, {@code false} otherwise
     */
    private static boolean isValidCoordinate(Coordinate coordinate) {
        return coordinate.x <= 180 && coordinate.x >= -180 &&
                coordinate.y <= 90 && coordinate.y >= -90;
    }

    /**
     * Parses the DBF file, extracting the name property if defined, and associating
     * it with the appropriate Shapes.  Note, it is assumed that the Shapes in the given
     * List are in the same order as in the DBF file.
     *
     * @param dbfFile DBF File to parse
     * @return Shapes with their associated names
     * @throws IOException Can be thrown if there is a problem reading from the file
     */
    public static List<Map<String, Object>> parseDBFFile(InputStream dbfFile) throws IOException {
        DBFReader reader = new DBFReader(dbfFile);

        int numFields = reader.getFieldCount();
        int numRecords = reader.getRecordCount();

        List<String> fieldNames = new ArrayList<String>();

        for (int i = 0; i < numFields; i++) {
            DBFField field = reader.getField(i);
            fieldNames.add(field.getName().trim());
        }

        List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();

        int recordNumber = 0;

        Object[] record;
        while ((record = reader.nextRecord()) != null) {
            Map<String, Object> recordData = new HashMap<String, Object>();
            for (int i = 0; i < fieldNames.size(); i++) {
                recordData.put(fieldNames.get(i), record[i]);
            }
            records.add(recordData);
        }
        return records;
    }
}
