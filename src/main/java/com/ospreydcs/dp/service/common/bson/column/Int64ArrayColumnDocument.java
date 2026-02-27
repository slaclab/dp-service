package com.ospreydcs.dp.service.common.bson.column;

import com.google.protobuf.Message;
import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.common.DataValues;
import com.ospreydcs.dp.grpc.v1.common.Int64ArrayColumn;
import com.ospreydcs.dp.service.common.exception.DpException;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

@BsonDiscriminator(key = "_t", value = "int64ArrayColumn")
public class Int64ArrayColumnDocument extends ArrayColumnDocumentBase {

    private static final int INT64_SIZE_BYTES = 8;

    public static Int64ArrayColumnDocument fromInt64ArrayColumn(Int64ArrayColumn requestColumn) 
            throws DpException {
        Int64ArrayColumnDocument document = new Int64ArrayColumnDocument();
        document.setName(requestColumn.getName());
        
        // Set dimensions
        ArrayDimensionsDocument dimensionsDoc = 
                ArrayDimensionsDocument.fromArrayDimensions(requestColumn.getDimensions());
        document.setDimensions(dimensionsDoc);
        
        // Serialize array values to binary storage
        List<Long> values = requestColumn.getValuesList();
        int elementCount = dimensionsDoc.getElementCount();
        int sampleCount = values.size() / elementCount;
        
        byte[] binaryData = document.serializeValues(values, sampleCount);
        document.setBinaryData(binaryData);
        
        return document;
    }

    @Override
    protected int getElementSizeBytes() {
        return INT64_SIZE_BYTES;
    }

    @Override
    protected void writeValuesToBuffer(ByteBuffer buffer, Object values, int totalElements) 
            throws DpException {
        @SuppressWarnings("unchecked")
        List<Long> longValues = (List<Long>) values;
        
        if (longValues.size() != totalElements) {
            throw new DpException("Expected " + totalElements + " values but got " + longValues.size());
        }
        
        for (Long value : longValues) {
            buffer.putLong(value);
        }
    }

    @Override
    protected Message deserializeToProtobufColumn() throws DpException {
        Int64ArrayColumn.Builder builder = Int64ArrayColumn.newBuilder();
        
        // Set name
        builder.setName(getName() != null ? getName() : "");
        
        // Set dimensions
        if (getDimensions() != null) {
            builder.setDimensions(getDimensions().toArrayDimensions());
        }
        
        // Deserialize values from binary storage
        byte[] binaryData = getBinaryData();
        ByteBuffer buffer = ByteBuffer.wrap(binaryData).order(ByteOrder.LITTLE_ENDIAN);
        
        int totalElements = binaryData.length / INT64_SIZE_BYTES;
        for (int i = 0; i < totalElements; i++) {
            builder.addValues(buffer.getLong());
        }
        
        return builder.build();
    }

    @Override
    public void addColumnToBucket(DataBucket.Builder bucketBuilder) throws DpException {
        Int64ArrayColumn int64ArrayColumn = (Int64ArrayColumn) toProtobufColumn();
        DataValues dataValues = DataValues.newBuilder().setInt64ArrayColumn(int64ArrayColumn).build();
        bucketBuilder.setDataValues(dataValues);
    }
}