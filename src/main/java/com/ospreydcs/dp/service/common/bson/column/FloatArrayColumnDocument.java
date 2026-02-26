package com.ospreydcs.dp.service.common.bson.column;

import com.google.protobuf.Message;
import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.common.DataValues;
import com.ospreydcs.dp.grpc.v1.common.FloatArrayColumn;
import com.ospreydcs.dp.service.common.exception.DpException;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

@BsonDiscriminator(key = "_t", value = "floatArrayColumn")
public class FloatArrayColumnDocument extends ArrayColumnDocumentBase {

    private static final int FLOAT_SIZE_BYTES = 4;

    public static FloatArrayColumnDocument fromFloatArrayColumn(FloatArrayColumn requestColumn) 
            throws DpException {
        FloatArrayColumnDocument document = new FloatArrayColumnDocument();
        document.setName(requestColumn.getName());
        
        // Set dimensions
        ArrayDimensionsDocument dimensionsDoc = 
                ArrayDimensionsDocument.fromArrayDimensions(requestColumn.getDimensions());
        document.setDimensions(dimensionsDoc);
        
        // Serialize array values to binary storage
        List<Float> values = requestColumn.getValuesList();
        int elementCount = dimensionsDoc.getElementCount();
        int sampleCount = values.size() / elementCount;
        
        byte[] binaryData = document.serializeValues(values, sampleCount);
        document.setBinaryData(binaryData);
        
        return document;
    }

    @Override
    protected int getElementSizeBytes() {
        return FLOAT_SIZE_BYTES;
    }

    @Override
    protected void writeValuesToBuffer(ByteBuffer buffer, Object values, int totalElements) 
            throws DpException {
        @SuppressWarnings("unchecked")
        List<Float> floatValues = (List<Float>) values;
        
        if (floatValues.size() != totalElements) {
            throw new DpException("Expected " + totalElements + " values but got " + floatValues.size());
        }
        
        for (Float value : floatValues) {
            buffer.putFloat(value);
        }
    }

    @Override
    protected Message deserializeToProtobufColumn() throws DpException {
        FloatArrayColumn.Builder builder = FloatArrayColumn.newBuilder();
        
        // Set name
        builder.setName(getName() != null ? getName() : "");
        
        // Set dimensions
        if (getDimensions() != null) {
            builder.setDimensions(getDimensions().toArrayDimensions());
        }
        
        // Deserialize values from binary storage
        byte[] binaryData = getBinaryData();
        ByteBuffer buffer = ByteBuffer.wrap(binaryData).order(ByteOrder.LITTLE_ENDIAN);
        
        int totalElements = binaryData.length / FLOAT_SIZE_BYTES;
        for (int i = 0; i < totalElements; i++) {
            builder.addValues(buffer.getFloat());
        }
        
        return builder.build();
    }

    @Override
    public void addColumnToBucket(DataBucket.Builder bucketBuilder) throws DpException {
        FloatArrayColumn floatArrayColumn = (FloatArrayColumn) toProtobufColumn();
        DataValues dataValues = DataValues.newBuilder().setFloatArrayColumn(floatArrayColumn).build();
        bucketBuilder.setDataValues(dataValues);
    }
}