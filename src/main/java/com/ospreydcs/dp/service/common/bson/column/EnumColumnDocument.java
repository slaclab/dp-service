package com.ospreydcs.dp.service.common.bson.column;

import com.google.protobuf.Message;
import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.EnumColumn;
import com.ospreydcs.dp.service.common.exception.DpException;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;

@BsonDiscriminator(key = "_t", value = "enumColumn")
public class EnumColumnDocument extends ScalarColumnDocumentBase<Integer> {

    private String enumId;

    public String getEnumId() {
        return enumId;
    }

    public void setEnumId(String enumId) {
        this.enumId = enumId;
    }

    public static EnumColumnDocument fromEnumColumn(EnumColumn requestColumn) {
        EnumColumnDocument document = new EnumColumnDocument();
        document.setName(requestColumn.getName());
        document.setValues(requestColumn.getValuesList());
        document.setEnumId(requestColumn.getEnumId());
        return document;
    }

    @Override
    protected Message.Builder createColumnBuilder() {
        return EnumColumn.newBuilder();
    }

    @Override
    protected void addAllValuesToBuilder(Message.Builder builder) {
        ((EnumColumn.Builder) builder).addAllValues(this.getValues());
    }

    @Override
    protected DataValue createDataValueFromScalar(Integer value) {
        return DataValue.newBuilder().setIntValue(value).build();
    }

    @Override
    public void addColumnToBucket(DataBucket.Builder bucketBuilder) throws DpException {
        EnumColumn enumColumn = (EnumColumn) toProtobufColumn();
        bucketBuilder.setEnumColumn(enumColumn);
    }

    @Override
    public Message toProtobufColumn() {
        Message.Builder builder = createColumnBuilder();
        
        // Set name field using reflection (same as parent class)
        try {
            String safeName = (getName() != null) ? getName() : "";
            builder.getClass().getMethod("setName", String.class).invoke(builder, safeName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set name on column builder", e);
        }
        
        // Add values using inherited method
        addAllValuesToBuilder(builder);
        
        // Add enumId field specific to EnumColumn
        if (enumId != null) {
            ((EnumColumn.Builder) builder).setEnumId(enumId);
        }
        
        return builder.build();
    }
}