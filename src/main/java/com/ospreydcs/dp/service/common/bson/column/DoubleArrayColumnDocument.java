package com.ospreydcs.dp.service.common.bson.column;

import com.google.protobuf.Message;
import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.common.DataValues;
import com.ospreydcs.dp.grpc.v1.common.DoubleArrayColumn;
import com.ospreydcs.dp.service.common.exception.DpException;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

@BsonDiscriminator(key = "_t", value = "doubleArrayColumn")
public class DoubleArrayColumnDocument extends ArrayColumnDocumentBase {

    private static final int DOUBLE_SIZE_BYTES = 8;

    public static DoubleArrayColumnDocument fromDoubleArrayColumn(DoubleArrayColumn requestColumn) 
            throws DpException {
        DoubleArrayColumnDocument document = new DoubleArrayColumnDocument();
        document.setName(requestColumn.getName());
        
        // Set dimensions
        ArrayDimensionsDocument dimensionsDoc = 
                ArrayDimensionsDocument.fromArrayDimensions(requestColumn.getDimensions());
        document.setDimensions(dimensionsDoc);
        
        // Serialize array values to binary storage
        List<Double> values = requestColumn.getValuesList();
        int elementCount = dimensionsDoc.getElementCount();
        int sampleCount = values.size() / elementCount;
        
        byte[] binaryData = document.serializeValues(values, sampleCount);
        document.setBinaryData(binaryData);
        
        return document;
    }

    @Override
    protected int getElementSizeBytes() {
        return DOUBLE_SIZE_BYTES;
    }

    @Override
    protected void writeValuesToBuffer(ByteBuffer buffer, Object values, int totalElements) 
            throws DpException {
        @SuppressWarnings("unchecked")
        List<Double> doubleValues = (List<Double>) values;
        
        if (doubleValues.size() != totalElements) {
            throw new DpException("Expected " + totalElements + " values but got " + doubleValues.size());
        }
        
        for (Double value : doubleValues) {
            buffer.putDouble(value);
        }
    }

    @Override
    protected Message deserializeToProtobufColumn() throws DpException {
        DoubleArrayColumn.Builder builder = DoubleArrayColumn.newBuilder();
        
        // Set name
        builder.setName(getName() != null ? getName() : "");
        
        // Set dimensions
        if (getDimensions() != null) {
            builder.setDimensions(getDimensions().toArrayDimensions());
        }
        
        // Deserialize values from binary storage
        byte[] binaryData = getBinaryData();
        ByteBuffer buffer = ByteBuffer.wrap(binaryData).order(ByteOrder.LITTLE_ENDIAN);
        
        int totalElements = binaryData.length / DOUBLE_SIZE_BYTES;
        for (int i = 0; i < totalElements; i++) {
            builder.addValues(buffer.getDouble());
        }
        
        return builder.build();
    }

    @Override
    public void addColumnToBucket(DataBucket.Builder bucketBuilder) throws DpException {
        DoubleArrayColumn doubleArrayColumn = (DoubleArrayColumn) toProtobufColumn();
        DataValues dataValues = DataValues.newBuilder().setDoubleArrayColumn(doubleArrayColumn).build();
        bucketBuilder.setDataValues(dataValues);
    }
}