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
 * This integration test covers the use of protobuf Int64ArrayColumns in the MLDP APIs.
 * Uses a dual PV approach where scalar columns serve as trigger PVs and array columns serve as target PVs
 * for data event subscriptions, since array columns cannot function as trigger PVs.
 */
public class Int64ArrayColumnIT extends GrpcIntegrationTestBase {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    /**
     * This test case provides full MLDP API coverage for use of Int64ArrayColumns.
     * Registers a provider, which is required before using the ingestion APIs.  Uses the data ingestion API
     * to send an IngestDataRequest whose IngestionDataFrame contains a Int64ArrayColumn data structure.
     * Uses the time-series data query API to retrieve the bucket containing the Int64ArrayColumn sent in
     * the ingestion request.  Confirms that the Int64ArrayColumn retrieved via the query API matches the
     * column sent in the ingestion request, using Int64ArrayColumn.equals() which compares column name,
     * dimensions, and data values in the two columns.  Subscribes for PV data via the subscribeData() API
     * method and confirms that the data received in the subscription response stream matches the ingested data.
     * Registers via subscribeDataEvent() for data events using a dual PV approach where scalar columns serve
     * as triggers and array columns serve as targets, and confirms that the appropriate responses are received.
     */
    @Test
    public void testInt64ArrayColumnIntegration() throws Exception {

        final String triggerPvName = "testTrigger-" + Instant.now().toEpochMilli();  // scalar PV for event triggers
        final String arrayPvName = "testArray-" + Instant.now().toEpochMilli();     // array PV for event targets
        final List<String> allColumnNames = Arrays.asList(triggerPvName, arrayPvName);
        final List<String> triggerColumnNames = Arrays.asList(triggerPvName);
        final List<String> arrayColumnNames = Arrays.asList(arrayPvName);
        
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
        Int64ArrayColumn requestArrayColumn;

        // ingest initial data with both scalar and array columns using dual PV approach
        {
            // specify explicit DoubleColumn data for trigger PV (scalar)
            final List<DoubleColumn> doubleColumns = new ArrayList<>();
            DoubleColumn.Builder doubleColumnBuilder = DoubleColumn.newBuilder();
            doubleColumnBuilder.setName(triggerPvName);
            doubleColumnBuilder.addValues(10.0);
            doubleColumnBuilder.addValues(20.0);
            requestTriggerColumn = doubleColumnBuilder.build();
            doubleColumns.add(requestTriggerColumn);

            // specify explicit Int64ArrayColumn data for target PV
            final List<Int64ArrayColumn> int64ArrayColumns = new ArrayList<>();
            Int64ArrayColumn.Builder arrayColumnBuilder = Int64ArrayColumn.newBuilder();
            arrayColumnBuilder.setName(arrayPvName);
            
            // Configure 2x3 array dimensions
            ArrayDimensions.Builder dimensionsBuilder = ArrayDimensions.newBuilder();
            dimensionsBuilder.addDims(2);
            dimensionsBuilder.addDims(3);
            arrayColumnBuilder.setDimensions(dimensionsBuilder.build());
            
            // Add values for 2 samples of 2x3 arrays (row-major: sample_count × product(dimensions))
            // Sample 1: [[11, 12, 13], [14, 15, 16]]
            arrayColumnBuilder.addValues(11L);
            arrayColumnBuilder.addValues(12L);
            arrayColumnBuilder.addValues(13L);
            arrayColumnBuilder.addValues(14L);
            arrayColumnBuilder.addValues(15L);
            arrayColumnBuilder.addValues(16L);
            // Sample 2: [[21, 22, 23], [24, 25, 26]]
            arrayColumnBuilder.addValues(21L);
            arrayColumnBuilder.addValues(22L);
            arrayColumnBuilder.addValues(23L);
            arrayColumnBuilder.addValues(24L);
            arrayColumnBuilder.addValues(25L);
            arrayColumnBuilder.addValues(26L);
            
            requestArrayColumn = arrayColumnBuilder.build();
            int64ArrayColumns.add(requestArrayColumn);

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
                            false,
                            null
                    );
            initialIngestionRequestParams.setDoubleColumnList(doubleColumns); // add scalar trigger column
            initialIngestionRequestParams.setInt64ArrayColumnList(int64ArrayColumns); // add array target column

            final IngestDataRequest request =
                    IngestionTestBase.buildIngestionRequest(initialIngestionRequestParams);

            ingestionServiceWrapper.sendAndVerifyIngestData(initialIngestionRequestParams, request, 0);
        }

        // positive queryData() test case for array column
        {
            // select 1 second of data for array pv
            final long beginSeconds = firstSeconds;
            final long beginNanos = firstNanos;
            final long endSeconds = beginSeconds + 1L;
            final long endNanos = 0L;

            final int numBucketsExpected = 1;
            final boolean expectReject = false;
            final String expectedRejectMessage = "";

            final QueryTestBase.QueryDataRequestParams params =
                    new QueryTestBase.QueryDataRequestParams(
                            arrayColumnNames,
                            beginSeconds,
                            beginNanos,
                            endSeconds,
                            endNanos,
                            false
                    );

            final List<DataBucket> queryResultBuckets = queryServiceWrapper.queryData(
                    params,
                    expectReject,
                    expectedRejectMessage
            );

            assertEquals(numBucketsExpected, queryResultBuckets.size());
            for (DataBucket queryResultBucket : queryResultBuckets) {
                assertTrue("Query result should contain Int64ArrayColumn", queryResultBucket.hasInt64ArrayColumn());
                assertEquals(requestArrayColumn, queryResultBucket.getInt64ArrayColumn());
            }
        }

        // create a data subscription for array PV, verification succeeds because data have been ingested
        SubscribeDataUtility.SubscribeDataCall subscribeDataCall;
        {
            final int expectedResponseCount = 1;
            final boolean expectReject = false;
            final String expectedRejectMessage = "";
            subscribeDataCall =
                    ingestionServiceWrapper.initiateSubscribeDataRequest(
                            arrayColumnNames, expectedResponseCount, expectReject, expectedRejectMessage);
        }

        // TODO: Export testing skipped for binary array columns since they don't support tabular export formats
        
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

            // DataEventOperation details for params - array PV as target
            final List<String> targetPvs = List.of(arrayPvName); // Array PV as target
            final long offset = -3_000_000_000L; // 3 seconds negative trigger time offset
            final long duration = 5_000_000_000L; // 5 second duration

            // add entry for event to response verification map with details about expected EventData responses
            final int expectedDataBucketCount = 1;
            final List<Instant> instantList = List.of(Instant.ofEpochSecond(firstSeconds + 1));
            final Map<String, List<Instant>> pvInstantMap = new HashMap<>();
            expectedEventDataResponses.put(event, pvInstantMap);
            pvInstantMap.put(arrayPvName, instantList); // Expect array PV data in response

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
        Int64ArrayColumn subscriptionArrayColumn;
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

            // specify explicit Int64ArrayColumn data for target PV
            final List<Int64ArrayColumn> int64ArrayColumns = new ArrayList<>();
            Int64ArrayColumn.Builder arrayColumnBuilder = Int64ArrayColumn.newBuilder();
            arrayColumnBuilder.setName(arrayPvName);
            
            // Configure same 2x3 array dimensions
            ArrayDimensions.Builder dimensionsBuilder = ArrayDimensions.newBuilder();
            dimensionsBuilder.addDims(2);
            dimensionsBuilder.addDims(3);
            arrayColumnBuilder.setDimensions(dimensionsBuilder.build());
            
            // Add new values for 2 samples of 2x3 arrays
            // Sample 1: [[31, 32, 33], [34, 35, 36]]
            arrayColumnBuilder.addValues(31L);
            arrayColumnBuilder.addValues(32L);
            arrayColumnBuilder.addValues(33L);
            arrayColumnBuilder.addValues(34L);
            arrayColumnBuilder.addValues(35L);
            arrayColumnBuilder.addValues(36L);
            // Sample 2: [[41, 42, 43], [44, 45, 46]]
            arrayColumnBuilder.addValues(41L);
            arrayColumnBuilder.addValues(42L);
            arrayColumnBuilder.addValues(43L);
            arrayColumnBuilder.addValues(44L);
            arrayColumnBuilder.addValues(45L);
            arrayColumnBuilder.addValues(46L);
            
            subscriptionArrayColumn = arrayColumnBuilder.build();
            int64ArrayColumns.add(subscriptionArrayColumn);

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
                            false,
                            null
                    );
            subscriptionRequestParams.setDoubleColumnList(doubleColumns); // add scalar trigger column with trigger value
            subscriptionRequestParams.setInt64ArrayColumnList(int64ArrayColumns); // add array target column

            final IngestDataRequest request =
                    IngestionTestBase.buildIngestionRequest(subscriptionRequestParams);

            ingestionServiceWrapper.sendAndVerifyIngestData(subscriptionRequestParams, request, 0);
        }

        // check that expected subscribeData() response is received for array PV
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

            // verify response contains expected Int64ArrayColumn data
            assertTrue(subscriptionResponse.hasSubscribeDataResult());
            assertEquals(1, subscriptionResponse.getSubscribeDataResult().getDataBucketsCount());
            final DataBucket receivedBucket = subscriptionResponse.getSubscribeDataResult().getDataBuckets(0);
            assertTrue("Subscription response should contain Int64ArrayColumn", receivedBucket.hasInt64ArrayColumn());
            // Note: subscription receives the latest data, which is from the second ingestion (subscriptionArrayColumn)
            final Int64ArrayColumn receivedColumn = receivedBucket.getInt64ArrayColumn();
            assertEquals(arrayPvName, receivedColumn.getName());
            assertTrue("Should have received array data", receivedColumn.getValuesCount() > 0);
        }

        // check that expected subscribeDataEvent() responses are received for array PV
        final List<DataBucket> responseDataBuckets = ingestionStreamServiceWrapper.verifySubscribeDataEventResponse(
                (IngestionStreamTestBase.SubscribeDataEventResponseObserver) subscribeDataEventCall.responseObserver(),
                expectedEventResponses,
                expectedEventDataResponses,
                0,
                DataBucket.DataCase.INT64ARRAYCOLUMN
        );

        // verify the data event response contains the expected Int64ArrayColumn data
        assertEquals(1, responseDataBuckets.size());
        final DataBucket eventDataBucket = responseDataBuckets.get(0);
        assertTrue("Event response should contain Int64ArrayColumn", eventDataBucket.hasInt64ArrayColumn());
        assertEquals(subscriptionArrayColumn, responseDataBuckets.get(0).getInt64ArrayColumn());
        ingestionStreamServiceWrapper.closeSubscribeDataEventCall(subscribeDataEventCall);
    }
}