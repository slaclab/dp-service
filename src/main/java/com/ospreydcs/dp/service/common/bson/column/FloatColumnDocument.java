package com.ospreydcs.dp.service.common.bson.column;

import com.google.protobuf.Message;
import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.DataValues;
import com.ospreydcs.dp.grpc.v1.common.FloatColumn;
import com.ospreydcs.dp.service.common.exception.DpException;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;

@BsonDiscriminator(key = "_t", value = "floatColumn")
public class FloatColumnDocument extends ScalarColumnDocumentBase<Float> {

    public static FloatColumnDocument fromFloatColumn(FloatColumn requestColumn) {
        FloatColumnDocument document = new FloatColumnDocument();
        document.setName(requestColumn.getName());
        document.setValues(requestColumn.getValuesList());
        return document;
    }

    @Override
    protected Message.Builder createColumnBuilder() {
        return FloatColumn.newBuilder();
    }

    @Override
    protected void addAllValuesToBuilder(Message.Builder builder) {
        ((FloatColumn.Builder) builder).addAllValues(this.getValues());
    }

    @Override
    protected DataValue createDataValueFromScalar(Float value) {
        return DataValue.newBuilder().setFloatValue(value).build();
    }

    @Override
    public void addColumnToBucket(DataBucket.Builder bucketBuilder) throws DpException {
        FloatColumn floatColumn = (FloatColumn) toProtobufColumn();
        DataValues dataValues = DataValues.newBuilder().setFloatColumn(floatColumn).build();
        bucketBuilder.setDataValues(dataValues);
    }

}