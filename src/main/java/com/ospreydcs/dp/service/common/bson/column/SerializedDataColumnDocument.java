package com.ospreydcs.dp.service.common.bson.column;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.common.SerializedDataColumn;
import com.ospreydcs.dp.service.common.exception.DpException;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;

/**
 * BSON document class for SerializedDataColumn storage in MongoDB.
 * Each SerializedDataColumn contains an encoding type and a binary payload that is 
 * serialized by the client using their chosen format (protobuf, avro, json, etc.).
 * Uses BinaryColumnDocumentBase for efficient storage with inline/GridFS support.
 */
@BsonDiscriminator(key = "_t", value = "serializedDataColumn")
public class
SerializedDataColumnDocument extends BinaryColumnDocumentBase {

    private String encoding;

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public static SerializedDataColumnDocument fromSerializedDataColumn(SerializedDataColumn requestColumn) throws DpException {
        SerializedDataColumnDocument document = new SerializedDataColumnDocument();
        document.setName(requestColumn.getName());
        document.setEncoding(requestColumn.getEncoding());
        
        // Store binary payload directly - no serialization needed since client already provides binary data
        byte[] payload = requestColumn.getPayload().toByteArray();
        document.setBinaryData(payload);
        
        return document;
    }

    @Override
    protected Message deserializeToProtobufColumn() throws DpException {
        SerializedDataColumn.Builder builder = SerializedDataColumn.newBuilder();
        
        // Set name and encoding
        builder.setName(getName() != null ? getName() : "");
        builder.setEncoding(getEncoding() != null ? getEncoding() : "");
        
        // Set binary payload
        byte[] binaryData = getBinaryData();
        builder.setPayload(ByteString.copyFrom(binaryData));
        
        return builder.build();
    }

    @Override
    public void addColumnToBucket(DataBucket.Builder bucketBuilder) throws DpException {
        SerializedDataColumn serializedDataColumn = (SerializedDataColumn) toProtobufColumn();
        bucketBuilder.setSerializedDataColumn(serializedDataColumn);
    }
}