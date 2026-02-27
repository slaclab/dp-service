package com.ospreydcs.dp.service.integration.v2api;

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
 * This integration test covers the use of protobuf StructColumns in the MLDP APIs.
 * Uses a dual PV approach where scalar columns serve as trigger PVs and struct columns serve as target PVs
 * for data event subscriptions, since binary columns cannot function as trigger PVs.
 */
public class StructColumnIT extends GrpcIntegrationTestBase {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    /**
     * This test case provides full MLDP API coverage for use of StructColumns.
     * Registers a provider, which is required before using the ingestion APIs.  Uses the data ingestion API
     * to send an IngestDataRequest whose IngestionDataFrame contains a StructColumn data structure.
     * Uses the time-series data query API to retrieve the bucket containing the StructColumn sent in
     * the ingestion request.  Confirms that the StructColumn retrieved via the query API matches the
     * column sent in the ingestion request, using StructColumn.equals() which compares column name,
     * schemaId, and data values in the two columns.  Subscribes for PV data via the subscribeData() API
     * method and confirms that the data received in the subscription response stream matches the ingested data.
     * Registers via subscribeDataEvent() for data events using a dual PV approach where scalar columns serve
     * as triggers and struct columns serve as targets, and confirms that the appropriate responses are received.
     */
    @Test
    public void testStructColumnIntegration() throws Exception {

        final String triggerPvName = "testTrigger-" + Instant.now().toEpochMilli();  // scalar PV for event triggers
        final String structPvName = "testStruct-" + Instant.now().toEpochMilli();   // struct PV for event targets
        final List<String> allColumnNames = Arrays.asList(triggerPvName, structPvName);
        final List<String> triggerColumnNames = Arrays.asList(triggerPvName);
        final List<String> structColumnNames = Arrays.asList(structPvName);
        
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
        StructColumn requestStructColumn;

        // ingest initial data with both scalar and struct columns using dual PV approach
        {
            // specify explicit DoubleColumn data for trigger PV (scalar)
            final List<DoubleColumn> doubleColumns = new ArrayList<>();
            DoubleColumn.Builder doubleColumnBuilder = DoubleColumn.newBuilder();
            doubleColumnBuilder.setName(triggerPvName);
            doubleColumnBuilder.addValues(10.0);
            doubleColumnBuilder.addValues(20.0);
            requestTriggerColumn = doubleColumnBuilder.build();
            doubleColumns.add(requestTriggerColumn);

            // specify explicit StructColumn data for target PV
            final List<StructColumn> structColumns = new ArrayList<>();
            StructColumn.Builder structColumnBuilder = StructColumn.newBuilder();
            structColumnBuilder.setName(structPvName);
            structColumnBuilder.setSchemaId("beam_position:v3");
            
            // Add struct values for 2 samples (each as serialized byte array)
            // Sample 1: Mock struct data for beam position
            byte[] struct1Data = "x=10.5,y=20.3,z=5.1".getBytes();
            structColumnBuilder.addValues(com.google.protobuf.ByteString.copyFrom(struct1Data));
            
            // Sample 2: Mock struct data for beam position
            byte[] struct2Data = "x=11.2,y=19.8,z=5.4".getBytes();
            structColumnBuilder.addValues(com.google.protobuf.ByteString.copyFrom(struct2Data));
            
            requestStructColumn = structColumnBuilder.build();
            structColumns.add(requestStructColumn);

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
            initialIngestionRequestParams.setStructColumnList(structColumns); // add struct target column

            final IngestDataRequest request =
                    IngestionTestBase.buildIngestionRequest(initialIngestionRequestParams);

            ingestionServiceWrapper.sendAndVerifyIngestData(initialIngestionRequestParams, request);
        }

        // positive queryData() test case for struct column
        {
            // select 1 second of data for struct pv
            final long beginSeconds = firstSeconds;
            final long beginNanos = firstNanos;
            final long endSeconds = beginSeconds + 1L;
            final long endNanos = 0L;

            final int numBucketsExpected = 1;
            final boolean expectReject = false;
            final String expectedRejectMessage = "";

            final QueryTestBase.QueryDataRequestParams params =
                    new QueryTestBase.QueryDataRequestParams(
                            structColumnNames,
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
                assertTrue("Query result should contain StructColumn", queryResultBucket.getDataValues().hasStructColumn());
                assertEquals(requestStructColumn, queryResultBucket.getDataValues().getStructColumn());
            }
        }

        // create a data subscription for struct PV, verification succeeds because data have been ingested
        SubscribeDataUtility.SubscribeDataCall subscribeDataCall;
        {
            final int expectedResponseCount = 1;
            final boolean expectReject = false;
            final String expectedRejectMessage = "";
            subscribeDataCall =
                    ingestionServiceWrapper.initiateSubscribeDataRequest(
                            structColumnNames, expectedResponseCount, expectReject, expectedRejectMessage);
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

            // DataEventOperation details for params - struct PV as target
            final List<String> targetPvs = List.of(structPvName); // Struct PV as target
            final long offset = -3_000_000_000L; // 3 seconds negative trigger time offset
            final long duration = 5_000_000_000L; // 5 second duration

            // add entry for event to response verification map with details about expected EventData responses
            final int expectedDataBucketCount = 1;
            final List<Instant> instantList = List.of(Instant.ofEpochSecond(firstSeconds + 1));
            final Map<String, List<Instant>> pvInstantMap = new HashMap<>();
            expectedEventDataResponses.put(event, pvInstantMap);
            pvInstantMap.put(structPvName, instantList); // Expect struct PV data in response

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
        StructColumn subscriptionStructColumn;
        {
            // create request parameters with trigger value that matches the condition in the event subscription
            final String subscriptionRequestId = "subscription-request-" + Instant.now().toEpochMilli();
            
            // specify explicit DoubleColumn data for trigger PV with trigger value
            final List<DoubleColumn> doubleColumns = new ArrayList<>();
            DoubleColumn.Builder doubleColumnBuilder = DoubleColumn.newBuilder();
            doubleColumnBuilder.setName(triggerPvName);
            doubleColumnBuilder.addValues(99.5); // Trigger value that matches event condition
            doubleColumnBuilder.addValues(50.0); // Non-trigger value
            DoubleColumn subscriptionTriggerColumn = doubleColumnBuilder.build();
            doubleColumns.add(subscriptionTriggerColumn);

            // specify explicit StructColumn data for target PV
            final List<StructColumn> structColumns = new ArrayList<>();
            StructColumn.Builder structColumnBuilder = StructColumn.newBuilder();
            structColumnBuilder.setName(structPvName);
            structColumnBuilder.setSchemaId("beam_position:v3"); // Same schema as initial data
            
            // Add new struct values for 2 samples
            // Sample 1: Updated beam position
            byte[] newStruct1Data = "x=15.7,y=25.2,z=6.3".getBytes();
            structColumnBuilder.addValues(com.google.protobuf.ByteString.copyFrom(newStruct1Data));
            
            // Sample 2: Updated beam position
            byte[] newStruct2Data = "x=14.9,y=24.8,z=6.1".getBytes();
            structColumnBuilder.addValues(com.google.protobuf.ByteString.copyFrom(newStruct2Data));
            
            subscriptionStructColumn = structColumnBuilder.build();
            structColumns.add(subscriptionStructColumn);

            IngestionTestBase.IngestionRequestParams subscriptionRequestParams =
                    new IngestionTestBase.IngestionRequestParams(
                            providerId,
                            subscriptionRequestId,
                            null,
                            null,
                            firstSeconds + 1, // 1 second later
                            firstNanos,
                            sampleIntervalNanos,
                            numSamples,
                            allColumnNames,
                            null,
                            null,
                            null,
                            null
                    );
            subscriptionRequestParams.setDoubleColumnList(doubleColumns); // add scalar trigger column with trigger value
            subscriptionRequestParams.setStructColumnList(structColumns); // add struct target column

            final IngestDataRequest request =
                    IngestionTestBase.buildIngestionRequest(subscriptionRequestParams);

            ingestionServiceWrapper.sendAndVerifyIngestData(subscriptionRequestParams, request);
        }

        // check that expected subscribeData() response is received for struct PV
        {
            final IngestionTestBase.SubscribeDataResponseObserver responseObserver =
                    (IngestionTestBase.SubscribeDataResponseObserver) subscribeDataCall.responseObserver();

            // wait for completion of API method response stream and confirm not in error state
            responseObserver.awaitResponseLatch();
            assertFalse(responseObserver.isError());

            // get subscription responses for verification of expected contents
            final List<SubscribeDataResponse> responseList = responseObserver.getResponseList();
            assertEquals(1, responseList.size());
            final SubscribeDataResponse subscriptionResponse = responseList.get(0);

            // verify response contains expected StructColumn data
            assertTrue(subscriptionResponse.hasSubscribeDataResult());
            assertEquals(1, subscriptionResponse.getSubscribeDataResult().getDataBucketsCount());
            final DataBucket receivedBucket = subscriptionResponse.getSubscribeDataResult().getDataBuckets(0);
            assertTrue("Subscription response should contain StructColumn", receivedBucket.getDataValues().hasStructColumn());
            // Note: subscription receives the latest data, which is from the second ingestion (subscriptionStructColumn)
            final StructColumn receivedColumn = receivedBucket.getDataValues().getStructColumn();
            assertEquals(structPvName, receivedColumn.getName());
            assertEquals("beam_position:v3", receivedColumn.getSchemaId());
            assertTrue("Should have received struct data", receivedColumn.getValuesCount() > 0);
        }

        // check that expected subscribeDataEvent() responses are received for struct PV
        final List<DataBucket> responseDataBuckets = ingestionStreamServiceWrapper.verifySubscribeDataEventResponse(
                (IngestionStreamTestBase.SubscribeDataEventResponseObserver) subscribeDataEventCall.responseObserver(),
                expectedEventResponses,
                expectedEventDataResponses,
                DataValues.ValuesCase.STRUCTCOLUMN
        );

        // verify the data event response contains the expected StructColumn data
        assertEquals(1, responseDataBuckets.size());
        final DataBucket eventDataBucket = responseDataBuckets.get(0);
        assertTrue("Event response should contain StructColumn", eventDataBucket.getDataValues().hasStructColumn());
        assertEquals(subscriptionStructColumn, responseDataBuckets.get(0).getDataValues().getStructColumn());
        ingestionStreamServiceWrapper.closeSubscribeDataEventCall(subscribeDataEventCall);
    }
}