package com.ospreydcs.dp.service.common.bson.column;

import com.google.protobuf.Message;
import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.service.common.exception.DpException;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;

@BsonDiscriminator
public abstract class ColumnDocumentBase {

    // instance variables
    private String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * Converts this document to its corresponding protobuf column message.
     * Each branch of the hierarchy implements this differently:
     * - Scalar columns use incremental builder pattern
     * - Binary columns use direct deserialization pattern
     */
    public abstract Message toProtobufColumn();

    public byte[] toByteArray() {
        return toProtobufColumn().toByteArray();
    }

    /**
     * Adds the column to the supplied DataBucket.Builder for use in query result.
     *
     * @param bucketBuilder
     * @throws DpException
     */
    public abstract void addColumnToBucket(DataBucket.Builder bucketBuilder) throws DpException;
}
