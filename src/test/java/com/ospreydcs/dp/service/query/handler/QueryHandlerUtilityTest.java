package com.ospreydcs.dp.service.query.handler;

import com.ospreydcs.dp.grpc.v1.query.QueryDataRequest;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import com.ospreydcs.dp.service.query.QueryTestBase;
import org.junit.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class QueryHandlerUtilityTest extends QueryTestBase {

    @Test
    public void testValidateRequestEmptyColumnNameList() {
        List<String> columnNames = new ArrayList<>();
        Long nowSeconds = Instant.now().getEpochSecond();
        QueryDataRequestParams params = new QueryDataRequestParams(
                columnNames,
                nowSeconds,
                0L,
                nowSeconds + 1,
                0L);
        QueryDataRequest request = buildQueryDataRequest(params);
        ResultStatus result = QueryHandlerUtility.validateQuerySpecData(request.getQuerySpec());
        assertTrue(result.isError);
        assertTrue(result.msg.equals("columnName must be specified"));
    }

    @Test
    public void testValidateRequestEmptyColumnName() {
        List<String> columnNames = List.of("");
        Long nowSeconds = Instant.now().getEpochSecond();
        QueryDataRequestParams params = new QueryDataRequestParams(
                columnNames,
                nowSeconds,
                0L,
                nowSeconds + 1,
                0L);
        QueryDataRequest request = buildQueryDataRequest(params);
        ResultStatus result = QueryHandlerUtility.validateQuerySpecData(request.getQuerySpec());
        assertTrue(result.isError);
        assertTrue(result.msg.equals("columnNamesList contains empty string"));
    }

    @Test
    public void testValidateRequestUnspecifiedStartTime() {
        List<String> columnNames = List.of("pv_01");
        Long nowSeconds = Instant.now().getEpochSecond();
        QueryDataRequestParams params = new QueryDataRequestParams(
                columnNames,
                null,
                0L,
                nowSeconds + 1,
                0L);
        QueryDataRequest request = buildQueryDataRequest(params);
        ResultStatus result = QueryHandlerUtility.validateQuerySpecData(request.getQuerySpec());
        assertTrue(result.isError);
        assertTrue(result.msg.equals("startTime must be specified"));
    }

    @Test
    public void testValidateRequestUnspecifiedEndTime() {
        List<String> columnNames = List.of("pv_01");
        Long nowSeconds = Instant.now().getEpochSecond();
        QueryDataRequestParams params = new QueryDataRequestParams(
                columnNames,
                nowSeconds,
                0L,
                null,
                0L);
        QueryDataRequest request = buildQueryDataRequest(params);
        ResultStatus result = QueryHandlerUtility.validateQuerySpecData(request.getQuerySpec());
        assertTrue(result.isError);
        assertTrue(result.msg.equals("endTime must be specified"));
    }

    @Test
    public void testValidateRequestInvalidEndTimeSeconds() {
        List<String> columnNames = List.of("pv_01");
        Long nowSeconds = Instant.now().getEpochSecond();
        QueryDataRequestParams params = new QueryDataRequestParams(
                columnNames,
                nowSeconds,
                0L,
                nowSeconds - 1,
                0L);
        QueryDataRequest request = buildQueryDataRequest(params);
        ResultStatus result = QueryHandlerUtility.validateQuerySpecData(request.getQuerySpec());
        assertTrue(result.isError);
        assertTrue(result.msg.equals("endTime seconds must be >= startTime seconds"));
    }

    @Test
    public void testValidateRequestInvalidEndTimeNanos() {
        List<String> columnNames = List.of("pv_01");
        Long nowSeconds = Instant.now().getEpochSecond();
        QueryDataRequestParams params = new QueryDataRequestParams(
                columnNames,
                nowSeconds,
                200L,
                nowSeconds,
                100L);
        QueryDataRequest request = buildQueryDataRequest(params);
        ResultStatus result = QueryHandlerUtility.validateQuerySpecData(request.getQuerySpec());
        assertTrue(result.isError);
        assertTrue(result.msg.equals("endTime nanos must be > startTime nanos when seconds match"));
    }

}
