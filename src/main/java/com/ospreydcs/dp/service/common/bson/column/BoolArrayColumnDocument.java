package com.ospreydcs.dp.service.common.bson.column;

import com.google.protobuf.Message;
import com.ospreydcs.dp.grpc.v1.common.BoolArrayColumn;
import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.common.DataValues;
import com.ospreydcs.dp.service.common.exception.DpException;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

@BsonDiscriminator(key = "_t", value = "boolArrayColumn")
public class BoolArrayColumnDocument extends ArrayColumnDocumentBase {

    private static final int BOOL_SIZE_BYTES = 1;

    public static BoolArrayColumnDocument fromBoolArrayColumn(BoolArrayColumn requestColumn) 
            throws DpException {
        BoolArrayColumnDocument document = new BoolArrayColumnDocument();
        document.setName(requestColumn.getName());
        
        // Set dimensions
        ArrayDimensionsDocument dimensionsDoc = 
                ArrayDimensionsDocument.fromArrayDimensions(requestColumn.getDimensions());
        document.setDimensions(dimensionsDoc);
        
        // Serialize array values to binary storage
        List<Boolean> values = requestColumn.getValuesList();
        int elementCount = dimensionsDoc.getElementCount();
        int sampleCount = values.size() / elementCount;
        
        byte[] binaryData = document.serializeValues(values, sampleCount);
        document.setBinaryData(binaryData);
        
        return document;
    }

    @Override
    protected int getElementSizeBytes() {
        return BOOL_SIZE_BYTES;
    }

    @Override
    protected void writeValuesToBuffer(ByteBuffer buffer, Object values, int totalElements) 
            throws DpException {
        @SuppressWarnings("unchecked")
        List<Boolean> boolValues = (List<Boolean>) values;
        
        if (boolValues.size() != totalElements) {
            throw new DpException("Expected " + totalElements + " values but got " + boolValues.size());
        }
        
        for (Boolean value : boolValues) {
            buffer.put((byte) (value ? 1 : 0));
        }
    }

    @Override
    protected Message deserializeToProtobufColumn() throws DpException {
        BoolArrayColumn.Builder builder = BoolArrayColumn.newBuilder();
        
        // Set name
        builder.setName(getName() != null ? getName() : "");
        
        // Set dimensions
        if (getDimensions() != null) {
            builder.setDimensions(getDimensions().toArrayDimensions());
        }
        
        // Deserialize values from binary storage
        byte[] binaryData = getBinaryData();
        ByteBuffer buffer = ByteBuffer.wrap(binaryData).order(ByteOrder.LITTLE_ENDIAN);
        
        int totalElements = binaryData.length / BOOL_SIZE_BYTES;
        for (int i = 0; i < totalElements; i++) {
            builder.addValues(buffer.get() != 0);
        }
        
        return builder.build();
    }

    @Override
    public void addColumnToBucket(DataBucket.Builder bucketBuilder) throws DpException {
        BoolArrayColumn boolArrayColumn = (BoolArrayColumn) toProtobufColumn();
        DataValues dataValues = DataValues.newBuilder().setBoolArrayColumn(boolArrayColumn).build();
        bucketBuilder.setDataValues(dataValues);
    }
}