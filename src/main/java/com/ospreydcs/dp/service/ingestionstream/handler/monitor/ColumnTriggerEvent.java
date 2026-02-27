package com.ospreydcs.dp.service.ingestionstream.handler.monitor;

import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.ingestionstream.PvConditionTrigger;

public record ColumnTriggerEvent (
        Timestamp triggerTimestamp,
        PvConditionTrigger trigger,
        DataValue dataValue
) {}