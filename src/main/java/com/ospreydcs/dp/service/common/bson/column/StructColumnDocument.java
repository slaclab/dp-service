package com.ospreydcs.dp.service.common.bson.column;

import com.google.protobuf.Message;
import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.common.StructColumn;
import com.ospreydcs.dp.service.common.exception.DpException;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;

/**
 * BSON document class for StructColumn storage in MongoDB.
 * Each StructColumn contains a schemaId and a list of struct values serialized as byte arrays.
 * Uses BinaryColumnDocumentBase for efficient storage of potentially large struct payloads.
 */
@BsonDiscriminator(key = "_t", value = "structColumn")
public class StructColumnDocument extends BinaryColumnDocumentBase {

    private String schemaId;

    public String getSchemaId() {
        return schemaId;
    }

    public void setSchemaId(String schemaId) {
        this.schemaId = schemaId;
    }

    public static StructColumnDocument fromStructColumn(StructColumn requestColumn) throws DpException {
        StructColumnDocument document = new StructColumnDocument();
        document.setName(requestColumn.getName());
        document.setSchemaId(requestColumn.getSchemaId());
        
        // Serialize struct values to binary storage
        // Calculate total size needed for all struct values
        int totalSize = 0;
        for (com.google.protobuf.ByteString structValue : requestColumn.getValuesList()) {
            totalSize += Integer.BYTES; // 4 bytes for length prefix
            totalSize += structValue.size(); // size of struct data
        }
        
        // Create binary array with length-prefixed struct values
        byte[] binaryData = new byte[totalSize];
        int offset = 0;
        
        for (com.google.protobuf.ByteString structValue : requestColumn.getValuesList()) {
            byte[] structBytes = structValue.toByteArray();
            int structSize = structBytes.length;
            
            // Write length prefix (4 bytes, little-endian)
            binaryData[offset] = (byte) (structSize & 0xFF);
            binaryData[offset + 1] = (byte) ((structSize >> 8) & 0xFF);
            binaryData[offset + 2] = (byte) ((structSize >> 16) & 0xFF);
            binaryData[offset + 3] = (byte) ((structSize >> 24) & 0xFF);
            offset += 4;
            
            // Write struct data
            System.arraycopy(structBytes, 0, binaryData, offset, structSize);
            offset += structSize;
        }
        
        document.setBinaryData(binaryData);
        return document;
    }

    @Override
    protected Message deserializeToProtobufColumn() throws DpException {
        StructColumn.Builder builder = StructColumn.newBuilder();
        
        // Set name and schemaId
        builder.setName(getName() != null ? getName() : "");
        builder.setSchemaId(getSchemaId() != null ? getSchemaId() : "");
        
        // Deserialize struct values from binary storage
        byte[] binaryData = getBinaryData();
        int offset = 0;
        
        while (offset < binaryData.length) {
            if (offset + 4 > binaryData.length) {
                throw new DpException("Invalid binary data: insufficient bytes for length prefix");
            }
            
            // Read length prefix (4 bytes, little-endian)
            int structSize = (binaryData[offset] & 0xFF) |
                            ((binaryData[offset + 1] & 0xFF) << 8) |
                            ((binaryData[offset + 2] & 0xFF) << 16) |
                            ((binaryData[offset + 3] & 0xFF) << 24);
            offset += 4;
            
            if (offset + structSize > binaryData.length) {
                throw new DpException("Invalid binary data: insufficient bytes for struct data");
            }
            
            // Read struct data
            byte[] structBytes = new byte[structSize];
            System.arraycopy(binaryData, offset, structBytes, 0, structSize);
            offset += structSize;
            
            builder.addValues(com.google.protobuf.ByteString.copyFrom(structBytes));
        }
        
        return builder.build();
    }

    @Override
    public void addColumnToBucket(DataBucket.Builder bucketBuilder) throws DpException {
        StructColumn structColumn = (StructColumn) toProtobufColumn();
        bucketBuilder.setStructColumn(structColumn);
    }
}