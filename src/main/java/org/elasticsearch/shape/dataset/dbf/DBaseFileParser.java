package org.elasticsearch.shape.dataset.dbf;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.*;

/**
 * http://www.clicketyclick.dk/databases/xbase/format/dbf.html
 */
@SuppressWarnings("unused")
public class DBaseFileParser {

    public static final Charset CHARSET = Charset.forName("8859_1");

    private static final byte FIELD_END = (byte) 0x0D;
    private static final byte DELETED = 0x2A;
    private static final byte VALID = 0x20;
    private static final byte END_OF_FILE = 0x1A;

    private final ByteBuffer byteBuffer;
    private final int numRecords;
    private final List<RecordField> fields;

    public DBaseFileParser(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
        this.numRecords = parseHeader(byteBuffer);
        this.fields = Collections.unmodifiableList(parseFields(byteBuffer));
    }

    public List<RecordField> fields() {
        return fields;
    }

    public Iterator<Object[]> records() {
        return new RecordIterator(numRecords, fields, byteBuffer);
    }

    private int parseHeader(ByteBuffer byteBuffer) {
        byte version = byteBuffer.get();
        byte lastUpdateYear = byteBuffer.get();
        byte lastUpdateMonth = byteBuffer.get();
        byte lastUpdateDay = byteBuffer.get();

        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        int numRecords = byteBuffer.getInt();
        short headerBytes = byteBuffer.getShort();
        short recordBytes = byteBuffer.getShort();
        byteBuffer.order(ByteOrder.BIG_ENDIAN);

        skipReserved(2, byteBuffer);

        byte incompleteTransaction = byteBuffer.get();
        byte encryptionFlag = byteBuffer.get();
        skipReserved(12, byteBuffer);
        byte mdxFlag = byteBuffer.get();
        byte langDriverId = byteBuffer.get();
        skipReserved(2, byteBuffer);

        return numRecords;
    }

    private List<RecordField> parseFields(ByteBuffer byteBuffer) {
        List<RecordField> fields = new ArrayList<RecordField>();

        while (true) {
            String fieldName = readNullFilledString(byteBuffer, 11);
            if (fieldName == null) {
                break;
            }

            byte dataType = byteBuffer.get();
            skipReserved(4, byteBuffer);
            byte fieldLength = byteBuffer.get();
            byte decimalCount = byteBuffer.get();
            skipReserved(2, byteBuffer);
            byte workAreaID = byteBuffer.get();
            skipReserved(2, byteBuffer);
            byte setFlagsField = byteBuffer.get();
            skipReserved(7, byteBuffer);
            byte indexFieldFlag = byteBuffer.get();

            fields.add(RecordField.getRecordFieldForType(dataType, fieldName, fieldLength));
        }

        return fields;
    }

    private void skipReserved(int numBytes, ByteBuffer byteBuffer) {
        for (int i = 0; i < numBytes; i++) {
            byteBuffer.get();
        }
    }

    private String readNullFilledString(ByteBuffer byteBuffer, int length) {
        byte mark = byteBuffer.get();
        if (mark == FIELD_END) {
            return null;
        }

        byte[] name = new byte[length];
        byteBuffer.get(name, 1, length - 1);
        name[0] = mark;

        int end = 0;
        for (int i = 0; i < name.length; i++) {
            if (name[i] == '\0') {
                end = i;
                break;
            }
        }

        return (end != 0) ? new String(name, 0, end, CHARSET) : null;
    }

    private static class RecordIterator implements Iterator<Object[]> {

        private final int numRecords;
        private final List<RecordField> fields;
        private final ByteBuffer byteBuffer;
        private int count;

        private RecordIterator(int numRecords, List<RecordField> fields, ByteBuffer byteBuffer) {
            this.numRecords = numRecords;
            this.fields = fields;
            this.byteBuffer = byteBuffer;
        }

        @Override
        public boolean hasNext() {
            return count < numRecords;
        }

        @Override
        public Object[] next() {
            byte marker = byteBuffer.get();
            if (marker == END_OF_FILE) {
                throw new NoSuchElementException("No more records");
            } else if (marker == DELETED) {
                return null;
            } else if (marker != VALID) {
                throw new IllegalStateException("Unexpected marker byte[" + marker + "]");
            }

            Object[] values = new Object[fields.size()];
            for (int i = 0; i < fields.size(); i++) {
                values[i] = fields.get(i).value(byteBuffer);
            }
            count++;
            return values;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Cannot remove from this iterator");
        }
    }
}
