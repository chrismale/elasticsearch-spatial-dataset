package org.elasticsearch.shape.dataset.dbf;

import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public abstract class RecordField {

    private final String name;
    protected final byte length;

    protected RecordField(String name, byte length) {
        this.name = name;
        this.length = length;
    }

    public String name() {
        return name;
    }

    public abstract Object value(ByteBuffer byteBuffer);

    protected String readString(ByteBuffer byteBuffer, int length) {
        byte[] name = new byte[length];
        byteBuffer.get(name, 0, length);
        return new String(name, DBaseFileParser.CHARSET);
    }

    public static RecordField getRecordFieldForType(byte type, String name, byte length) {
        switch (type) {
            case 'C':
                return new CharacterField(name, length);
            case 'N':
                return new NumberField(name, length);
            case 'L':
                return new LogicalField(name, length);
            case 'D':
                return new DateField(name, length);
            case 'F':
                return new FloatField(name, length);
            default:
                throw new UnsupportedOperationException("Field [" + name + "] has unsupported type [" + type + "]");
        }
    }

    public static class CharacterField extends RecordField {

        public CharacterField(String name, byte length) {
            super(name, length);
        }

        @Override
        public Object value(ByteBuffer byteBuffer) {
            return readString(byteBuffer, length);
        }
    }

    public static class NumberField extends RecordField {

        public NumberField(String name, byte length) {
            super(name, length);
        }

        @Override
        public Object value(ByteBuffer byteBuffer) {
            String number = readString(byteBuffer, length).trim();
            return (number.length() > 0) ? Double.parseDouble(number) : null;
        }
    }

    public static class LogicalField extends RecordField {

        public LogicalField(String name, byte length) {
            super(name, length);
        }

        @Override
        public Object value(ByteBuffer byteBuffer) {
            byte value = byteBuffer.get();
            return value == 'Y' || value == 'y' || value == 'T' || value == 't';
        }
    }

    public static class DateField extends RecordField {

        private static final String DATE_PATTERN = "yyyyMMdd";

        public DateField(String name, byte length) {
            super(name, length);
        }

        @Override
        public Object value(ByteBuffer byteBuffer) {
            try {
                SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_PATTERN);
                return dateFormat.parse(readString(byteBuffer, length));
            } catch (ParseException pe) {
                throw new RuntimeException(pe);
            }
        }
    }

    public static class FloatField extends RecordField {

        public FloatField(String name, byte length) {
            super(name, length);
        }

        @Override
        public Object value(ByteBuffer byteBuffer) {
            String number = readString(byteBuffer, length).trim();
            return (number.length() > 0) ? Float.parseFloat(number) : null;
        }
    }
}
