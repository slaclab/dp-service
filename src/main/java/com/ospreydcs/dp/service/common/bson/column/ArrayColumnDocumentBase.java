package com.ospreydcs.dp.service.common.bson.column;

import com.google.protobuf.Message;
import com.ospreydcs.dp.service.common.exception.DpException;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Abstract base class for array column document types (DoubleArrayColumn, FloatArrayColumn, etc.).
 * Extends BinaryColumnDocumentBase to leverage storage abstraction while adding array-specific
 * functionality like dimensions and binary serialization logic.
 */
@BsonDiscriminator
public abstract class ArrayColumnDocumentBase extends BinaryColumnDocumentBase {

    private ArrayDimensionsDocument dimensions;

    public ArrayDimensionsDocument getDimensions() {
        return dimensions;
    }

    public void setDimensions(ArrayDimensionsDocument dimensions) {
        this.dimensions = dimensions;
    }

    /**
     * Returns the number of elements in one array sample (product of all dimensions).
     */
    public int getElementCount() {
        return dimensions != null ? dimensions.getElementCount() : 0;
    }

    /**
     * Returns the size in bytes of one primitive element for this array type.
     * Subclasses must implement this to specify their element size.
     */
    protected abstract int getElementSizeBytes();

    /**
     * Serializes array values to binary format for storage.
     * Uses little-endian byte order and row-major flattening.
     * 
     * @param values The flattened array values (sample_count × element_count)
     * @param sampleCount The number of samples in the time series
     * @return Binary representation of the array data
     */
    protected byte[] serializeValues(Object values, int sampleCount) throws DpException {
        if (dimensions == null) {
            throw new DpException("Array dimensions not set");
        }

        int elementCount = getElementCount();
        int elementSize = getElementSizeBytes();
        int totalElements = sampleCount * elementCount;
        
        ByteBuffer buffer = ByteBuffer.allocate(totalElements * elementSize)
                                    .order(ByteOrder.LITTLE_ENDIAN);
        
        // Delegate to subclass to write values to buffer
        writeValuesToBuffer(buffer, values, totalElements);
        
        return buffer.array();
    }

    /**
     * Writes the array values to the ByteBuffer.
     * Subclasses implement this with type-specific logic.
     * 
     * @param buffer The ByteBuffer to write to (already configured with little-endian order)
     * @param values The array values to write
     * @param totalElements Total number of elements to write
     */
    protected abstract void writeValuesToBuffer(ByteBuffer buffer, Object values, int totalElements) 
            throws DpException;

    /**
     * Reads array values from binary storage and creates the appropriate protobuf column.
     * Subclasses implement this with type-specific deserialization logic.
     */
    protected abstract Message deserializeToProtobufColumn() throws DpException;

    @Override
    protected Message.Builder createColumnBuilder() {
        // Subclasses will override toProtobufColumn() directly
        throw new UnsupportedOperationException("Array columns should override toProtobufColumn() directly");
    }

    @Override
    protected void addAllValuesToBuilder(Message.Builder builder) {
        // Not used by array columns - they override toProtobufColumn() directly
        throw new UnsupportedOperationException("Array columns should override toProtobufColumn() directly");
    }

    @Override
    public Message toProtobufColumn() {
        try {
            return deserializeToProtobufColumn();
        } catch (DpException e) {
            throw new RuntimeException("Failed to deserialize array column", e);
        }
    }
}