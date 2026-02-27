package com.ospreydcs.dp.service.common.bson.column;

import com.google.protobuf.Message;
import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.DataValues;
import com.ospreydcs.dp.grpc.v1.common.StringColumn;
import com.ospreydcs.dp.service.common.exception.DpException;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;

@BsonDiscriminator(key = "_t", value = "stringColumn")
public class StringColumnDocument extends ScalarColumnDocumentBase<String> {

    public static StringColumnDocument fromStringColumn(StringColumn requestColumn) {
        StringColumnDocument document = new StringColumnDocument();
        document.setName(requestColumn.getName());
        document.setValues(requestColumn.getValuesList());
        return document;
    }

    @Override
    protected Message.Builder createColumnBuilder() {
        return StringColumn.newBuilder();
    }

    @Override
    protected void addAllValuesToBuilder(Message.Builder builder) {
        ((StringColumn.Builder) builder).addAllValues(this.getValues());
    }

    @Override
    protected DataValue createDataValueFromScalar(String value) {
        return DataValue.newBuilder().setStringValue(value).build();
    }

    @Override
    public void addColumnToBucket(DataBucket.Builder bucketBuilder) throws DpException {
        StringColumn stringColumn = (StringColumn) toProtobufColumn();
        DataValues dataValues = DataValues.newBuilder().setStringColumn(stringColumn).build();
        bucketBuilder.setDataValues(dataValues);
    }

}