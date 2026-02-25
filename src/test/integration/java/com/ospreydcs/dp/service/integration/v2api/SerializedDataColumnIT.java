package com.ospreydcs.dp.service.integration.v2api;

import com.google.protobuf.ByteString;
import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataResponse;
import com.ospreydcs.dp.grpc.v1.ingestionstream.PvConditionTrigger;
import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventResponse;
import com.ospreydcs.dp.service.ingest.IngestionTestBase;
import com.ospreydcs.dp.service.ingest.utility.SubscribeDataUtility;
import com.ospreydcs.dp.service.ingestionstream.IngestionStreamTestBase;
import com.ospreydcs.dp.service.integration.GrpcIntegrationTestBase;
import com.ospreydcs.dp.service.query.QueryTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

import java.time.Instant;
import java.util.*;

/**
 * This integration test covers the use of protobuf SerializedDataColumns in the MLDP APIs.
 * Uses a dual PV approach where scalar columns serve as trigger PVs and serialized data columns serve as target PVs
 * for data event subscriptions, since binary columns cannot function as trigger PVs.
 */
public class SerializedDataColumnIT extends GrpcIntegrationTestBase {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    /**
     * This test case provides full MLDP API coverage for use of SerializedDataColumns.
     * Registers a provider, which is required before using the ingestion APIs.  Uses the data ingestion API
     * to send an IngestDataRequest whose IngestionDataFrame contains a SerializedDataColumn data structure.
     * Uses the time-series data query API to retrieve the bucket containing the SerializedDataColumn sent in
     * the ingestion request.  Confirms that the SerializedDataColumn retrieved via the query API matches the
     * column sent in the ingestion request, using SerializedDataColumn.equals() which compares column name,
     * encoding, and binary payload in the two columns.  Subscribes for PV data via the subscribeData() API
     * method and confirms that the data received in the subscription response stream matches the ingested data.
     * Registers via subscribeDataEvent() for data events using a dual PV approach where scalar columns serve
     * as triggers and serialized data columns serve as targets, and confirms that the appropriate responses are received.
     */
    @Test
    public void testSerializedDataColumnIntegration() throws Exception {

        final String triggerPvName = "testTrigger-" + Instant.now().toEpochMilli();  // scalar PV for event triggers
        final String serializedPvName = "testSerialized-" + Instant.now().toEpochMilli();   // serialized data PV for event targets
        final List<String> allColumnNames = Arrays.asList(triggerPvName, serializedPvName);
        final List<String> triggerColumnNames = Arrays.asList(triggerPvName);
        final List<String> serializedColumnNames = Arrays.asList(serializedPvName);
        
        String providerId;
        {
            // register ingestion provider
            final String providerName = String.valueOf(1);
            providerId = ingestionServiceWrapper.registerProvider(providerName, null);
        }
        
        final String requestId = "test-request-" + Instant.now().toEpochMilli();

        final Long firstSeconds = 100L;
        final Long firstNanos = 0L;
        final Long sampleIntervalNanos = 25L; // 40 MHz
        final Integer numSamples = 2;

        IngestionTestBase.IngestionRequestParams initialIngestionRequestParams;
        DoubleColumn requestTriggerColumn;
        SerializedDataColumn requestSerializedColumn;

        // ingest initial data with both scalar and serialized data columns using dual PV approach
        {
            // specify explicit DoubleColumn data for trigger PV (scalar)
            final List<DoubleColumn> doubleColumns = new ArrayList<>();
            DoubleColumn.Builder doubleColumnBuilder = DoubleColumn.newBuilder();
            doubleColumnBuilder.setName(triggerPvName);
            doubleColumnBuilder.addValues(10.0);
            doubleColumnBuilder.addValues(20.0);
            requestTriggerColumn = doubleColumnBuilder.build();
            doubleColumns.add(requestTriggerColumn);

            // specify explicit SerializedDataColumn data for target PV
            final List<SerializedDataColumn> serializedColumns = new ArrayList<>();
            SerializedDataColumn.Builder serializedColumnBuilder = SerializedDataColumn.newBuilder();
            serializedColumnBuilder.setName(serializedPvName);
            serializedColumnBuilder.setEncoding("application/json");
            
            // Create sample JSON payloads for testing - combining multiple JSON objects for 2 samples
            String json1 = "{\"sensor_id\": \"temp_01\", \"value\": 23.5, \"unit\": \"celsius\"}";
            String json2 = "{\"sensor_id\": \"temp_01\", \"value\": 24.1, \"unit\": \"celsius\"}";
            String combinedPayload = json1 + "\n" + json2; // Two JSON objects separated by newline
            
            serializedColumnBuilder.setPayload(ByteString.copyFromUtf8(combinedPayload));
            requestSerializedColumn = serializedColumnBuilder.build();
            serializedColumns.add(requestSerializedColumn);

            // create request parameters for both columns
            initialIngestionRequestParams =
                    new IngestionTestBase.IngestionRequestParams(
                            providerId,
                            requestId,
                            null,
                            null,
                            firstSeconds,
                            firstNanos,
                            sampleIntervalNanos,
                            numSamples,
                            allColumnNames,
                            null,
                            null,
                            null,
                            null
                    );
            initialIngestionRequestParams.setDoubleColumnList(doubleColumns); // add scalar trigger column
            initialIngestionRequestParams.setSerializedDataColumnList(serializedColumns); // add serialized target column

            final IngestDataRequest request =
                    IngestionTestBase.buildIngestionRequest(initialIngestionRequestParams);

            ingestionServiceWrapper.sendAndVerifyIngestData(initialIngestionRequestParams, request);
        }

        // positive queryData() test case for serialized data column
        {
            // select 1 second of data for serialized data pv
            final long beginSeconds = firstSeconds;
            final long beginNanos = firstNanos;
            final long endSeconds = beginSeconds + 1L;
            final long endNanos = 0L;

            final int numBucketsExpected = 1;
            final boolean expectReject = false;
            final String expectedRejectMessage = "";

            final QueryTestBase.QueryDataRequestParams params =
                    new QueryTestBase.QueryDataRequestParams(
                            serializedColumnNames,
                            beginSeconds,
                            beginNanos,
                            endSeconds,
                            endNanos
                    );

            final List<DataBucket> queryResultBuckets = queryServiceWrapper.queryData(
                    params,
                    expectReject,
                    expectedRejectMessage
            );

            assertEquals(numBucketsExpected, queryResultBuckets.size());
            for (DataBucket queryResultBucket : queryResultBuckets) {
                assertTrue("Query result should contain SerializedDataColumn", queryResultBucket.hasSerializedDataColumn());
                assertEquals(requestSerializedColumn, queryResultBucket.getSerializedDataColumn());
            }
        }

        // create a data subscription for serialized data PV, verification succeeds because data have been ingested
        SubscribeDataUtility.SubscribeDataCall subscribeDataCall;
        {
            final int expectedResponseCount = 1;
            final boolean expectReject = false;
            final String expectedRejectMessage = "";
            subscribeDataCall =
                    ingestionServiceWrapper.initiateSubscribeDataRequest(
                            serializedColumnNames, expectedResponseCount, expectReject, expectedRejectMessage);
        }

        // TODO: Export testing skipped for binary columns since they don't support tabular export formats
        
        // create a data event subscription using dual PV approach
        IngestionStreamTestBase.SubscribeDataEventCall subscribeDataEventCall;
        IngestionStreamTestBase.SubscribeDataEventRequestParams requestParams;
        Map<PvConditionTrigger, List<SubscribeDataEventResponse.Event>> expectedEventResponses = new HashMap<>();
        Map<SubscribeDataEventResponse.Event, Map<String, List<Instant>>> expectedEventDataResponses = new HashMap<>();
        int expectedEventResponseCount = 0;
        {
            // create list of triggers for request using scalar PV as trigger
            List<PvConditionTrigger> requestTriggers = new ArrayList<>();

            // create trigger using scalar PV
            SubscribeDataEventResponse.Event event;
            {
                PvConditionTrigger trigger = PvConditionTrigger.newBuilder()
                        .setPvName(triggerPvName) // Use scalar PV as trigger
                        .setCondition(PvConditionTrigger.PvCondition.PV_CONDITION_EQUAL_TO)
                        .setValue(DataValue.newBuilder().setDoubleValue(99.5).build())
                        .build();
                requestTriggers.add(trigger);

                // add entry to response verification map with trigger and expected TriggeredEvent responses
                final List<SubscribeDataEventResponse.Event> triggerExpectedEvents = new ArrayList<>();
                final DataValue eventDataValue = DataValue.newBuilder().setDoubleValue(99.5).build();
                event = SubscribeDataEventResponse.Event.newBuilder()
                        .setTrigger(trigger)
                        .setDataValue(eventDataValue)
                        .setEventTime(Timestamp.newBuilder().setEpochSeconds(firstSeconds+1).build())
                        .build();
                triggerExpectedEvents.add(event);
                expectedEventResponses.put(trigger, triggerExpectedEvents);
                expectedEventResponseCount += triggerExpectedEvents.size();
            }

            // DataEventOperation details for params - serialized data PV as target
            final List<String> targetPvs = List.of(serializedPvName); // Serialized data PV as target
            final long offset = -3_000_000_000L; // 3 seconds negative trigger time offset
            final long duration = 5_000_000_000L; // 5 second duration

            // add entry for event to response verification map with details about expected EventData responses
            final int expectedDataBucketCount = 1;
            final List<Instant> instantList = List.of(Instant.ofEpochSecond(firstSeconds + 1));
            final Map<String, List<Instant>> pvInstantMap = new HashMap<>();
            expectedEventDataResponses.put(event, pvInstantMap);
            pvInstantMap.put(serializedPvName, instantList); // Expect serialized data PV data in response

            // create params object (including trigger params list) for building protobuf request from params
            requestParams =
                    new IngestionStreamTestBase.SubscribeDataEventRequestParams(
                            requestTriggers,
                            targetPvs,
                            offset,
                            duration);

            // call subscribeDataEvent() to initiate subscription before running ingestion

            final boolean expectReject = false;
            final String expectedRejectMessage = "";
            subscribeDataEventCall = ingestionStreamServiceWrapper.initiateSubscribeDataEventRequest(
                    requestParams,
                    expectedEventResponseCount,
                    expectedDataBucketCount,
                    expectReject,
                    expectedRejectMessage);
        }

        // ingest additional data to trigger the data event subscription, using scalar PV trigger value
        SerializedDataColumn subscriptionSerializedColumn;
        {
            // create request parameters with trigger value that matches the condition in the event subscription
            final String triggerRequestId = "trigger-request-" + Instant.now().toEpochMilli();
            
            // specify explicit DoubleColumn data for trigger PV (scalar) with trigger value 99.5
            final List<DoubleColumn> triggerDoubleColumns = new ArrayList<>();
            DoubleColumn.Builder triggerDoubleColumnBuilder = DoubleColumn.newBuilder();
            triggerDoubleColumnBuilder.setName(triggerPvName);
            triggerDoubleColumnBuilder.addValues(99.5); // This matches our trigger condition
            triggerDoubleColumnBuilder.addValues(100.0);
            DoubleColumn triggerColumn = triggerDoubleColumnBuilder.build();
            triggerDoubleColumns.add(triggerColumn);

            // specify explicit SerializedDataColumn data for target PV (different data for subscription)
            final List<SerializedDataColumn> triggerSerializedColumns = new ArrayList<>();
            SerializedDataColumn.Builder triggerSerializedColumnBuilder = SerializedDataColumn.newBuilder();
            triggerSerializedColumnBuilder.setName(serializedPvName);
            triggerSerializedColumnBuilder.setEncoding("application/json");
            
            // Create new payload for the subscription trigger
            String triggerJson1 = "{\"sensor_id\": \"temp_02\", \"value\": 25.0, \"unit\": \"celsius\"}";
            String triggerJson2 = "{\"sensor_id\": \"temp_02\", \"value\": 25.5, \"unit\": \"celsius\"}";
            String triggerPayload = triggerJson1 + "\n" + triggerJson2;
            
            triggerSerializedColumnBuilder.setPayload(ByteString.copyFromUtf8(triggerPayload));
            subscriptionSerializedColumn = triggerSerializedColumnBuilder.build();
            triggerSerializedColumns.add(subscriptionSerializedColumn);

            final IngestionTestBase.IngestionRequestParams triggerIngestionRequestParams =
                    new IngestionTestBase.IngestionRequestParams(
                            providerId,
                            triggerRequestId,
                            null,
                            null,
                            firstSeconds + 1,  // 1 second later
                            firstNanos,
                            sampleIntervalNanos,
                            numSamples,
                            allColumnNames,
                            null,
                            null,
                            null,
                            null
                    );
            triggerIngestionRequestParams.setDoubleColumnList(triggerDoubleColumns);
            triggerIngestionRequestParams.setSerializedDataColumnList(triggerSerializedColumns);

            final IngestDataRequest triggerRequest = IngestionTestBase.buildIngestionRequest(triggerIngestionRequestParams);
            ingestionServiceWrapper.sendAndVerifyIngestData(triggerIngestionRequestParams, triggerRequest);
        }

        // check that expected subscribeData() response is received for serialized data PV
        {
            final IngestionTestBase.SubscribeDataResponseObserver responseObserver =
                    (IngestionTestBase.SubscribeDataResponseObserver) subscribeDataCall.responseObserver();
            // wait for completion of API method response stream and confirm not in error state
            responseObserver.awaitResponseLatch();
            assertFalse(responseObserver.isError());

            assertEquals(1, responseObserver.getResponseList().size());
            final SubscribeDataResponse subscriptionResponse = responseObserver.getResponseList().get(0);
            assertTrue(subscriptionResponse.hasSubscribeDataResult());
            assertEquals(1, subscriptionResponse.getSubscribeDataResult().getDataBucketsCount());
            final DataBucket receivedBucket = subscriptionResponse.getSubscribeDataResult().getDataBuckets(0);
            assertTrue("Subscription response should contain SerializedDataColumn", receivedBucket.hasSerializedDataColumn());
            // Note: subscription receives the latest data, which is from the second ingestion (subscriptionSerializedColumn)
            assertEquals(subscriptionSerializedColumn, receivedBucket.getSerializedDataColumn());
        }

        // check that expected subscribeDataEvent() responses are received for serialized data PV
        final List<DataBucket> responseDataBuckets = ingestionStreamServiceWrapper.verifySubscribeDataEventResponse(
                (IngestionStreamTestBase.SubscribeDataEventResponseObserver) subscribeDataEventCall.responseObserver(),
                expectedEventResponses,
                expectedEventDataResponses,
                DataBucket.DataCase.SERIALIZEDDATACOLUMN
        );

        // additional verification for event response
        assertEquals(1, responseDataBuckets.size());
        final DataBucket eventDataBucket = responseDataBuckets.get(0);
        assertTrue("Event response should contain SerializedDataColumn", eventDataBucket.hasSerializedDataColumn());
        assertEquals(subscriptionSerializedColumn, responseDataBuckets.get(0).getSerializedDataColumn());
        ingestionStreamServiceWrapper.closeSubscribeDataEventCall(subscribeDataEventCall);
    }
}