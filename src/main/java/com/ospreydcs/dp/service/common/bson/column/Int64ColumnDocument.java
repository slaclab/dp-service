package com.ospreydcs.dp.service.common.bson.column;

import com.google.protobuf.Message;
import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.DataValues;
import com.ospreydcs.dp.grpc.v1.common.Int64Column;
import com.ospreydcs.dp.service.common.exception.DpException;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;

@BsonDiscriminator(key = "_t", value = "int64Column")
public class Int64ColumnDocument extends ScalarColumnDocumentBase<Long> {

    public static Int64ColumnDocument fromInt64Column(Int64Column requestColumn) {
        Int64ColumnDocument document = new Int64ColumnDocument();
        document.setName(requestColumn.getName());
        document.setValues(requestColumn.getValuesList());
        return document;
    }

    @Override
    protected Message.Builder createColumnBuilder() {
        return Int64Column.newBuilder();
    }

    @Override
    protected void addAllValuesToBuilder(Message.Builder builder) {
        ((Int64Column.Builder) builder).addAllValues(this.getValues());
    }

    @Override
    protected DataValue createDataValueFromScalar(Long value) {
        return DataValue.newBuilder().setLongValue(value).build();
    }

    @Override
    public void addColumnToBucket(DataBucket.Builder bucketBuilder) throws DpException {
        Int64Column int64Column = (Int64Column) toProtobufColumn();
        DataValues dataValues = DataValues.newBuilder().setInt64Column(int64Column).build();
        bucketBuilder.setDataValues(dataValues);
    }

}