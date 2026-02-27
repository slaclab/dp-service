package com.ospreydcs.dp.service.common.bson.column;

import com.google.protobuf.Message;
import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.common.DataValues;
import com.ospreydcs.dp.grpc.v1.common.Int32ArrayColumn;
import com.ospreydcs.dp.service.common.exception.DpException;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

@BsonDiscriminator(key = "_t", value = "int32ArrayColumn")
public class Int32ArrayColumnDocument extends ArrayColumnDocumentBase {

    private static final int INT32_SIZE_BYTES = 4;

    public static Int32ArrayColumnDocument fromInt32ArrayColumn(Int32ArrayColumn requestColumn) 
            throws DpException {
        Int32ArrayColumnDocument document = new Int32ArrayColumnDocument();
        document.setName(requestColumn.getName());
        
        // Set dimensions
        ArrayDimensionsDocument dimensionsDoc = 
                ArrayDimensionsDocument.fromArrayDimensions(requestColumn.getDimensions());
        document.setDimensions(dimensionsDoc);
        
        // Serialize array values to binary storage
        List<Integer> values = requestColumn.getValuesList();
        int elementCount = dimensionsDoc.getElementCount();
        int sampleCount = values.size() / elementCount;
        
        byte[] binaryData = document.serializeValues(values, sampleCount);
        document.setBinaryData(binaryData);
        
        return document;
    }

    @Override
    protected int getElementSizeBytes() {
        return INT32_SIZE_BYTES;
    }

    @Override
    protected void writeValuesToBuffer(ByteBuffer buffer, Object values, int totalElements) 
            throws DpException {
        @SuppressWarnings("unchecked")
        List<Integer> intValues = (List<Integer>) values;
        
        if (intValues.size() != totalElements) {
            throw new DpException("Expected " + totalElements + " values but got " + intValues.size());
        }
        
        for (Integer value : intValues) {
            buffer.putInt(value);
        }
    }

    @Override
    protected Message deserializeToProtobufColumn() throws DpException {
        Int32ArrayColumn.Builder builder = Int32ArrayColumn.newBuilder();
        
        // Set name
        builder.setName(getName() != null ? getName() : "");
        
        // Set dimensions
        if (getDimensions() != null) {
            builder.setDimensions(getDimensions().toArrayDimensions());
        }
        
        // Deserialize values from binary storage
        byte[] binaryData = getBinaryData();
        ByteBuffer buffer = ByteBuffer.wrap(binaryData).order(ByteOrder.LITTLE_ENDIAN);
        
        int totalElements = binaryData.length / INT32_SIZE_BYTES;
        for (int i = 0; i < totalElements; i++) {
            builder.addValues(buffer.getInt());
        }
        
        return builder.build();
    }

    @Override
    public void addColumnToBucket(DataBucket.Builder bucketBuilder) throws DpException {
        Int32ArrayColumn int32ArrayColumn = (Int32ArrayColumn) toProtobufColumn();
        DataValues dataValues = DataValues.newBuilder().setInt32ArrayColumn(int32ArrayColumn).build();
        bucketBuilder.setDataValues(dataValues);
    }
}