package com.ospreydcs.dp.service.ingestionstream.handler.monitor;

import com.google.protobuf.InvalidProtocolBufferException;
import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.grpc.v1.ingestionstream.PvConditionTrigger;
import com.ospreydcs.dp.service.common.protobuf.DataTimestampsUtility;
import com.ospreydcs.dp.service.common.protobuf.TimestampUtility;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * This class provides static utility methods for use by the subscribeDataEvent() handling framework's EventMonitor. The
 * core feature is to check if data received for a PV in the subscribeData() response stream triggers the event for the
 * corresponding subscription's PvConditionTriggers.  The primary (public) checkColumnTrigger() variant includes a
 * DataBucket parameter and invokes the appropriate checkColumnTrigger() private variant for the concrete protobuf
 * column data type.
 */
public class ColumnTriggerUtility {

    private static record ValueTriggerResult(
            boolean isTriggered,
            boolean isError,
            String errorMsg
    ) {}

    /**
     * This generic method can be used to evaluate relational operators for any Java Comparable type, and is used for
     * various protobuf column data types to check if an individual data value triggers the condition.
     *
     * @param typedDataValue
     * @param typedTriggerValue
     * @param triggerCondition
     * @return
     * @param <T>
     */
    private static <T extends Comparable<T>> ValueTriggerResult checkValueTrigger(
            T typedDataValue,
            T typedTriggerValue,
            PvConditionTrigger.PvCondition triggerCondition
    ) {
        final int compareResult = typedDataValue.compareTo(typedTriggerValue);

        switch (triggerCondition) {
            case PV_CONDITION_UNSPECIFIED -> {
                final String errorMsg = "PvConditionTrigger.condition must be specified";
                return new ValueTriggerResult(false, true, errorMsg);
            }
            case PV_CONDITION_EQUAL_TO -> {
                return new ValueTriggerResult(compareResult == 0, false, "");
            }
            case PV_CONDITION_GREATER -> {
                return new ValueTriggerResult(compareResult > 0, false, "");
            }
            case PV_CONDITION_GREATER_EQ -> {
                return new ValueTriggerResult(compareResult >= 0, false, "");
            }
            case PV_CONDITION_LESS -> {
                return new ValueTriggerResult(compareResult < 0, false, "");
            }
            case PV_CONDITION_LESS_EQ -> {
                return new ValueTriggerResult(compareResult <= 0, false, "");
            }
            case UNRECOGNIZED -> {
                final String errorMsg = "PvConditionTrigger.condition unrecognized enum value";
                return new ValueTriggerResult(false, true, errorMsg);
            }
        }

        final String errorMsg = "PvConditionTrigger.condition unhandled condition: " + triggerCondition;
        return new ValueTriggerResult(false, true, errorMsg);
    }

    /**
     * Uses private variant methods of the same name to check if the condition for the specified PvConditionTrigger
     * is triggered by data in the dataBucket's column payload.
     *
     * @param trigger
     * @param dataBucket
     * @return
     */
    public static ColumnTriggerResult checkColumnTrigger(
            PvConditionTrigger trigger,
            DataBucket dataBucket
    ) {
        switch (dataBucket.getDataCase()) {
            case DATACOLUMN -> {
                return checkColumnTrigger(trigger, dataBucket.getDataColumn(), dataBucket.getDataTimestamps());
            }
            case SERIALIZEDDATACOLUMN -> {
                final DataColumn dataColumn;
                try {
                    dataColumn = DataColumn.parseFrom(dataBucket.getSerializedDataColumn().getPayload());
                } catch (InvalidProtocolBufferException e) {
                    final String errorMsg = "InvalidProtocolBufferException msg: " + e.getMessage();
                    return new ColumnTriggerResult(true, errorMsg, null);
                }
                return checkColumnTrigger(trigger, dataColumn, dataBucket.getDataTimestamps());
            }
            case DOUBLECOLUMN -> {
                return checkColumnTrigger(trigger, dataBucket.getDoubleColumn(), dataBucket.getDataTimestamps());
            }
            case FLOATCOLUMN -> {
            }
            case INT64COLUMN -> {
            }
            case INT32COLUMN -> {
            }
            case BOOLCOLUMN -> {
            }
            case STRINGCOLUMN -> {
            }
            case ENUMCOLUMN -> {
            }
            case IMAGECOLUMN -> {
            }
            case STRUCTCOLUMN -> {
            }
            case DOUBLEARRAYCOLUMN -> {
            }
            case FLOATARRAYCOLUMN -> {
            }
            case INT32ARRAYCOLUMN -> {
            }
            case INT64ARRAYCOLUMN -> {
            }
            case BOOLARRAYCOLUMN -> {
            }
            case DATA_NOT_SET -> {
            }
        }

        final String errorMsg = "DataBucket.data column not set";
        return new ColumnTriggerResult(true, errorMsg, null);
    }

    /**
     * Checks if the contents of the supplied DataColumn trigger the condition of the supplied PvConditionTrigger.
     */
    private static ColumnTriggerResult checkColumnTrigger(
            PvConditionTrigger trigger,
            DataColumn column,
            DataTimestamps dataTimestamps
    ) {
        final String columnPvName = column.getName();
        final PvConditionTrigger.PvCondition triggerCondition = trigger.getCondition();
        final DataValue triggerValue = trigger.getValue();

        // check if each column data value triggers the event
        int columnValueIndex = 0;
        List<ColumnTriggerEvent> columnTriggerEvents = new ArrayList<>();
        for (DataValue dataValue : column.getDataValuesList()) {

            // check for type mismatch between column data value and trigger value
            if (dataValue.getValueCase() != triggerValue.getValueCase()) {
                final String errorMsg = "PvConditionTrigger type mismatch PV name: " + columnPvName
                        + " PV data type: " + dataValue.getValueCase().name()
                        + " trigger value data type: " + triggerValue.getValueCase().name();
                return new ColumnTriggerResult(true, errorMsg, null);
            }

            // check if event condition is triggered by data value
            boolean isTriggered = false;
            ValueTriggerResult valueTriggerResult = null;
            switch (dataValue.getValueCase()) {

                case STRINGVALUE -> {
                    final String typedDataValue = dataValue.getStringValue();
                    final String typedTriggerValue = triggerValue.getStringValue();
                    valueTriggerResult = checkValueTrigger(typedDataValue, typedTriggerValue, triggerCondition);
                }

                case BOOLEANVALUE -> {
                    final Boolean typedDataValue = dataValue.getBooleanValue();
                    final Boolean typedTriggerValue = triggerValue.getBooleanValue();
                    valueTriggerResult = checkValueTrigger(typedDataValue, typedTriggerValue, triggerCondition);
                }

                case UINTVALUE -> {
                    final int typedDataValue = dataValue.getUintValue();
                    final int typedTriggerValue = triggerValue.getUintValue();
                    valueTriggerResult = checkValueTrigger(typedDataValue, typedTriggerValue, triggerCondition);
                }

                case ULONGVALUE -> {
                    final long typedDataValue = dataValue.getUlongValue();
                    final long typedTriggerValue = triggerValue.getUlongValue();
                    valueTriggerResult = checkValueTrigger(typedDataValue, typedTriggerValue, triggerCondition);
                }

                case INTVALUE -> {
                    final int typedDataValue = dataValue.getIntValue();
                    final int typedTriggerValue = triggerValue.getIntValue();
                    valueTriggerResult = checkValueTrigger(typedDataValue, typedTriggerValue, triggerCondition);
                }

                case LONGVALUE -> {
                    final long typedDataValue = dataValue.getLongValue();
                    final long typedTriggerValue = triggerValue.getLongValue();
                    valueTriggerResult = checkValueTrigger(typedDataValue, typedTriggerValue, triggerCondition);
                }

                case FLOATVALUE -> {
                    final float typedDataValue = dataValue.getFloatValue();
                    final float typedTriggerValue = triggerValue.getFloatValue();
                    valueTriggerResult = checkValueTrigger(typedDataValue, typedTriggerValue, triggerCondition);
                }

                case DOUBLEVALUE -> {
                    final double typedDataValue = dataValue.getDoubleValue();
                    final double typedTriggerValue = triggerValue.getDoubleValue();
                    valueTriggerResult = checkValueTrigger(typedDataValue, typedTriggerValue, triggerCondition);
                }

                case TIMESTAMPVALUE -> {
                    final Instant dataValueInstant =
                            TimestampUtility.instantFromTimestamp(dataValue.getTimestampValue());
                    final Instant triggerValueInstant =
                            TimestampUtility.instantFromTimestamp(triggerValue.getTimestampValue());
                    valueTriggerResult = checkValueTrigger(dataValueInstant, triggerValueInstant, triggerCondition);
                }

                case BYTEARRAYVALUE, ARRAYVALUE, STRUCTUREVALUE, IMAGEVALUE -> {
                    final String errorMsg = "PvConditionTrigger PV data type not supported: " + columnPvName
                            + " PV data type: " + dataValue.getValueCase().name();
                    return new ColumnTriggerResult(true, errorMsg, null);
                }

                case VALUE_NOT_SET -> {
                    final String errorMsg = "PvConditionTrigger PV data type not specified: " + columnPvName;
                    return new ColumnTriggerResult(true, errorMsg, null);
                }

            }

            boolean isError = false;
            if (valueTriggerResult != null) {
                isTriggered = valueTriggerResult.isTriggered();
                isError = valueTriggerResult.isError();
                if (isError) {
                    final String errorMsg = "PvConditionTrigger error comparing data value for PV name: "
                            + columnPvName
                            + " msg: " + valueTriggerResult.errorMsg();
                    return new ColumnTriggerResult(true, errorMsg, null);
                }
            }

            if (isTriggered) {
                final Timestamp triggerTimestamp =
                        DataTimestampsUtility.timestampForIndex(dataTimestamps, columnValueIndex);
                if (triggerTimestamp == null) {
                    final String errorMsg = "PvConditionTrigger error getting timestamp for PV: " + columnPvName;
                    return new ColumnTriggerResult(true, errorMsg, null);
                }

                // Add details for triggered event to result.
                columnTriggerEvents.add(new ColumnTriggerEvent(triggerTimestamp, trigger, dataValue));
            }

            columnValueIndex = columnValueIndex + 1;
        }

        return new ColumnTriggerResult(false, "", columnTriggerEvents);
    }

/**
 * Checks if the contents of the supplied DoubleColumn trigger the condition of the supplied PvConditionTrigger.
 */
 private static ColumnTriggerResult checkColumnTrigger(
            PvConditionTrigger trigger,
            DoubleColumn column,
            DataTimestamps dataTimestamps
    ) {
        final String columnPvName = column.getName();
        final PvConditionTrigger.PvCondition triggerCondition = trigger.getCondition();
        final DataValue triggerValue = trigger.getValue();

        if (triggerValue.getValueCase() != DataValue.ValueCase.DOUBLEVALUE) {
            final String errorMsg = "PvConditionTrigger type mismatch PV name: " + columnPvName
                    + " trigger value data type expected double: " + triggerValue.getValueCase().name();
            return new ColumnTriggerResult(true, errorMsg, null);
        }

        // check if each column data value triggers the event
        int columnValueIndex = 0;
        List<ColumnTriggerEvent> columnTriggerEvents = new ArrayList<>();
        for (double doubleValue : column.getValuesList()) {

            final double typedTriggerValue = triggerValue.getDoubleValue();
            ValueTriggerResult valueTriggerResult = checkValueTrigger(doubleValue, typedTriggerValue, triggerCondition);

            boolean isError = false;
            boolean isTriggered = false;
            if (valueTriggerResult != null) {
                isTriggered = valueTriggerResult.isTriggered();
                isError = valueTriggerResult.isError();
                if (isError) {
                    final String errorMsg = "PvConditionTrigger error comparing data value for PV name: "
                            + columnPvName
                            + " msg: " + valueTriggerResult.errorMsg();
                    return new ColumnTriggerResult(true, errorMsg, null);
                }
            }

            if (isTriggered) {
                final Timestamp triggerTimestamp =
                        DataTimestampsUtility.timestampForIndex(dataTimestamps, columnValueIndex);
                if (triggerTimestamp == null) {
                    final String errorMsg = "PvConditionTrigger error getting timestamp for PV: " + columnPvName;
                    return new ColumnTriggerResult(true, errorMsg, null);
                }

                // Add details for triggered event to result.
                DataValue doubleDataValue = DataValue.newBuilder().setDoubleValue(doubleValue).build();
                columnTriggerEvents.add(new ColumnTriggerEvent(triggerTimestamp, trigger, doubleDataValue));
            }

            columnValueIndex = columnValueIndex + 1;
        }

        return new ColumnTriggerResult(false, "", columnTriggerEvents);
    }
}
