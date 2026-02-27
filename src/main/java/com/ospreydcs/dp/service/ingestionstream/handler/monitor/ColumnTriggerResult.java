package com.ospreydcs.dp.service.ingestionstream.handler.monitor;

import java.util.List;

public record ColumnTriggerResult (
        boolean isError,
        String errorMsg,
        List<ColumnTriggerEvent> columnTriggerEvents
) {}