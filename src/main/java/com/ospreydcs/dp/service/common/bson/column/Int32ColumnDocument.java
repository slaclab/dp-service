package com.ospreydcs.dp.service.common.bson.column;

import com.google.protobuf.Message;
import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.DataValues;
import com.ospreydcs.dp.grpc.v1.common.Int32Column;
import com.ospreydcs.dp.service.common.exception.DpException;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;

@BsonDiscriminator(key = "_t", value = "int32Column")
public class Int32ColumnDocument extends ScalarColumnDocumentBase<Integer> {

    public static Int32ColumnDocument fromInt32Column(Int32Column requestColumn) {
        Int32ColumnDocument document = new Int32ColumnDocument();
        document.setName(requestColumn.getName());
        document.setValues(requestColumn.getValuesList());
        return document;
    }

    @Override
    protected Message.Builder createColumnBuilder() {
        return Int32Column.newBuilder();
    }

    @Override
    protected void addAllValuesToBuilder(Message.Builder builder) {
        ((Int32Column.Builder) builder).addAllValues(this.getValues());
    }

    @Override
    protected DataValue createDataValueFromScalar(Integer value) {
        return DataValue.newBuilder().setIntValue(value).build();
    }

    @Override
    public void addColumnToBucket(DataBucket.Builder bucketBuilder) throws DpException {
        Int32Column int32Column = (Int32Column) toProtobufColumn();
        DataValues dataValues = DataValues.newBuilder().setInt32Column(int32Column).build();
        bucketBuilder.setDataValues(dataValues);
    }

}