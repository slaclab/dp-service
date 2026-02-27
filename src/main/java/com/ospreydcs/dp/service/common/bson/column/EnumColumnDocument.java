package com.ospreydcs.dp.service.common.bson.column;

import com.google.protobuf.Message;
import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.DataValues;
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
        DataValues dataValues = DataValues.newBuilder().setEnumColumn(enumColumn).build();
        bucketBuilder.setDataValues(dataValues);
    }

    @Override
    protected void customizeBuilder(Message.Builder builder) {
        // Add enumId field specific to EnumColumn
        if (enumId != null) {
            ((EnumColumn.Builder) builder).setEnumId(enumId);
        }
    }
}