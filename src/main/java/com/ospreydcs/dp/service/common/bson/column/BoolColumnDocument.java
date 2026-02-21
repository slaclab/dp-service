package com.ospreydcs.dp.service.common.bson.column;

import com.google.protobuf.Message;
import com.ospreydcs.dp.grpc.v1.common.BoolColumn;
import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.service.common.exception.DpException;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;

@BsonDiscriminator(key = "_t", value = "boolColumn")
public class BoolColumnDocument extends ScalarColumnDocumentBase<Boolean> {

    public static BoolColumnDocument fromBoolColumn(BoolColumn requestColumn) {
        BoolColumnDocument document = new BoolColumnDocument();
        document.setName(requestColumn.getName());
        document.setValues(requestColumn.getValuesList());
        return document;
    }

    @Override
    protected Message.Builder createColumnBuilder() {
        return BoolColumn.newBuilder();
    }

    @Override
    protected void addAllValuesToBuilder(Message.Builder builder) {
        ((BoolColumn.Builder) builder).addAllValues(this.getValues());
    }

    @Override
    protected DataValue createDataValueFromScalar(Boolean value) {
        return DataValue.newBuilder().setBooleanValue(value).build();
    }

    @Override
    public void addColumnToBucket(DataBucket.Builder bucketBuilder) throws DpException {
        BoolColumn boolColumn = (BoolColumn) toProtobufColumn();
        bucketBuilder.setBoolColumn(boolColumn);
    }

}