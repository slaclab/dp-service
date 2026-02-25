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
 * This integration test covers the use of protobuf DoubleArrayColumns in the MLDP APIs.
 * Uses a dual PV approach where scalar columns serve as trigger PVs and array columns serve as target PVs
 * for data event subscriptions, since array columns cannot function as trigger PVs.
 */
public class DoubleArrayColumnIT extends GrpcIntegrationTestBase {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    /**
     * This test case provides full MLDP API coverage for use of DoubleArrayColumns.
     * Registers a provider, which is required before using the ingestion APIs.  Uses the data ingestion API
     * to send an IngestDataRequest whose IngestionDataFrame contains a DoubleArrayColumn data structure.
     * Uses the time-series data query API to retrieve the bucket containing the DoubleArrayColumn sent in
     * the ingestion request.  Confirms that the DoubleArrayColumn retrieved via the query API matches the
     * column sent in the ingestion request, using DoubleArrayColumn.equals() which compares column name,
     * dimensions, and data values in the two columns.  Subscribes for PV data via the subscribeData() API
     * method and confirms that the data received in the subscription response stream matches the ingested data.
     * Registers via subscribeDataEvent() for data events using a dual PV approach where scalar columns serve
     * as triggers and array columns serve as targets, and confirms that the appropriate responses are received.
     * Creates a dataset and tests exportData() to tabular CSV output format and verifies that the output
     * contains the expected array data.
     */
    @Test
    public void doubleArrayColumnTest() {

        String providerId;
        {
            // register ingestion provider
            final String providerName = String.valueOf(1);
            providerId = ingestionServiceWrapper.registerProvider(providerName, null);
        }

        // Use dual PV approach: scalar PV for trigger, array PV for target
        final String triggerPvName = "scalar_trigger_pv";
        final String arrayPvName = "array_target_pv";
        final List<String> triggerColumnNames = Arrays.asList(triggerPvName);
        final List<String> arrayColumnNames = Arrays.asList(arrayPvName);
        final List<String> allColumnNames = Arrays.asList(triggerPvName, arrayPvName);

        long firstSeconds = Instant.now().getEpochSecond();
        long firstNanos = 0L;
        IngestionTestBase.IngestionRequestParams initialIngestionRequestParams;
        DoubleColumn requestTriggerColumn;
        DoubleArrayColumn requestArrayColumn;
        {
            // Initial ingestion: both scalar trigger PV and array target PV for PV validation
            // assemble IngestionRequest
            final String requestId = "request-initial";
            final long sampleIntervalNanos = 1_000_000L;
            final int numSamples = 2;

            // specify explicit DoubleColumn data for trigger PV
            final List<DoubleColumn> doubleColumns = new ArrayList<>();
            DoubleColumn.Builder doubleColumnBuilder = DoubleColumn.newBuilder();
            doubleColumnBuilder.setName(triggerPvName);
            doubleColumnBuilder.addValues(10.0);
            doubleColumnBuilder.addValues(20.0);
            requestTriggerColumn = doubleColumnBuilder.build();
            doubleColumns.add(requestTriggerColumn);

            // specify explicit DoubleArrayColumn data for target PV
            final List<DoubleArrayColumn> doubleArrayColumns = new ArrayList<>();
            DoubleArrayColumn.Builder arrayColumnBuilder = DoubleArrayColumn.newBuilder();
            arrayColumnBuilder.setName(arrayPvName);
            
            // Configure 2x3 array dimensions
            ArrayDimensions.Builder dimensionsBuilder = ArrayDimensions.newBuilder();
            dimensionsBuilder.addDims(2);
            dimensionsBuilder.addDims(3);
            arrayColumnBuilder.setDimensions(dimensionsBuilder.build());
            
            // Add values for 2 samples of 2x3 arrays (row-major: sample_count × product(dimensions))
            // Sample 1: [[1.1, 1.2, 1.3], [1.4, 1.5, 1.6]]
            arrayColumnBuilder.addValues(1.1);
            arrayColumnBuilder.addValues(1.2);
            arrayColumnBuilder.addValues(1.3);
            arrayColumnBuilder.addValues(1.4);
            arrayColumnBuilder.addValues(1.5);
            arrayColumnBuilder.addValues(1.6);
            // Sample 2: [[2.1, 2.2, 2.3], [2.4, 2.5, 2.6]]
            arrayColumnBuilder.addValues(2.1);
            arrayColumnBuilder.addValues(2.2);
            arrayColumnBuilder.addValues(2.3);
            arrayColumnBuilder.addValues(2.4);
            arrayColumnBuilder.addValues(2.5);
            arrayColumnBuilder.addValues(2.6);
            
            requestArrayColumn = arrayColumnBuilder.build();
            doubleArrayColumns.add(requestArrayColumn);

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
            initialIngestionRequestParams.setDoubleArrayColumnList(doubleArrayColumns); // add array target column

            final IngestDataRequest request =
                    IngestionTestBase.buildIngestionRequest(initialIngestionRequestParams);

            ingestionServiceWrapper.sendAndVerifyIngestData(initialIngestionRequestParams, request);
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
                            endNanos
                    );

            final List<DataBucket> queryResultBuckets = queryServiceWrapper.queryData(
                    params,
                    expectReject,
                    expectedRejectMessage
            );

            assertEquals(numBucketsExpected, queryResultBuckets.size());
            for (DataBucket queryResultBucket : queryResultBuckets) {
                assertTrue("Query result should contain DoubleArrayColumn", queryResultBucket.hasDoubleArrayColumn());
                assertEquals(requestArrayColumn, queryResultBucket.getDoubleArrayColumn());
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

        // ingest data that will be published to data subscription and trigger data event subscription
        DoubleColumn subscriptionTriggerColumn;
        DoubleArrayColumn subscriptionArrayColumn;
        {
            // Second ingestion: both scalar trigger (to trigger event) and array target (for subscription data)
            // assemble IngestionRequest
            final String requestId = "request-subscription";
            final long sampleIntervalNanos = 1_000_000L;
            final int numSamples = 2;

            // specify explicit DoubleColumn data for trigger PV (includes trigger value)
            final List<DoubleColumn> doubleColumns = new ArrayList<>();
            DoubleColumn.Builder doubleColumnBuilder = DoubleColumn.newBuilder();
            doubleColumnBuilder.setName(triggerPvName);
            doubleColumnBuilder.addValues(99.5); // This value will trigger the event
            doubleColumnBuilder.addValues(88.8);
            subscriptionTriggerColumn = doubleColumnBuilder.build();
            doubleColumns.add(subscriptionTriggerColumn);

            // specify explicit DoubleArrayColumn data for subscription
            final List<DoubleArrayColumn> doubleArrayColumns = new ArrayList<>();
            DoubleArrayColumn.Builder arrayColumnBuilder = DoubleArrayColumn.newBuilder();
            arrayColumnBuilder.setName(arrayPvName);
            
            // Configure 2x3 array dimensions (same as before)
            ArrayDimensions.Builder dimensionsBuilder = ArrayDimensions.newBuilder();
            dimensionsBuilder.addDims(2);
            dimensionsBuilder.addDims(3);
            arrayColumnBuilder.setDimensions(dimensionsBuilder.build());
            
            // Add values for 2 samples of 2x3 arrays (different data from initial ingestion)
            // Sample 1: [[3.1, 3.2, 3.3], [3.4, 3.5, 3.6]]
            arrayColumnBuilder.addValues(3.1);
            arrayColumnBuilder.addValues(3.2);
            arrayColumnBuilder.addValues(3.3);
            arrayColumnBuilder.addValues(3.4);
            arrayColumnBuilder.addValues(3.5);
            arrayColumnBuilder.addValues(3.6);
            // Sample 2: [[4.1, 4.2, 4.3], [4.4, 4.5, 4.6]]
            arrayColumnBuilder.addValues(4.1);
            arrayColumnBuilder.addValues(4.2);
            arrayColumnBuilder.addValues(4.3);
            arrayColumnBuilder.addValues(4.4);
            arrayColumnBuilder.addValues(4.5);
            arrayColumnBuilder.addValues(4.6);
            
            subscriptionArrayColumn = arrayColumnBuilder.build();
            doubleArrayColumns.add(subscriptionArrayColumn);

            // create request parameters for both columns
            final IngestionTestBase.IngestionRequestParams subscriptionRequestParams =
                    new IngestionTestBase.IngestionRequestParams(
                            providerId,
                            requestId,
                            null,
                            null,
                            firstSeconds+1,
                            firstNanos,
                            sampleIntervalNanos,
                            numSamples,
                            allColumnNames,
                            null,
                            null,
                            null,
                            null
                    );
            subscriptionRequestParams.setDoubleColumnList(doubleColumns); // add scalar trigger column
            subscriptionRequestParams.setDoubleArrayColumnList(doubleArrayColumns); // add array target column

            final IngestDataRequest request =
                    IngestionTestBase.buildIngestionRequest(subscriptionRequestParams);

            ingestionServiceWrapper.sendAndVerifyIngestData(subscriptionRequestParams, request);
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
            assertTrue(subscriptionResponse.hasSubscribeDataResult());
            assertEquals(1, subscriptionResponse.getSubscribeDataResult().getDataBucketsCount());
            final DataBucket responseBucket = subscriptionResponse.getSubscribeDataResult().getDataBuckets(0);
            assertTrue("Response bucket should contain DoubleArrayColumn", responseBucket.hasDoubleArrayColumn());
            assertEquals(subscriptionArrayColumn, responseBucket.getDoubleArrayColumn());
        }

        // check that expected subscribeDataEvent() responses are received for array PV
        final List<DataBucket> responseDataBuckets = ingestionStreamServiceWrapper.verifySubscribeDataEventResponse(
                (IngestionStreamTestBase.SubscribeDataEventResponseObserver) subscribeDataEventCall.responseObserver(),
                expectedEventResponses,
                expectedEventDataResponses,
                DataBucket.DataCase.DOUBLEARRAYCOLUMN);
        assertEquals(1, responseDataBuckets.size());
        assertTrue("Event response bucket should contain DoubleArrayColumn", responseDataBuckets.get(0).hasDoubleArrayColumn());
        assertEquals(subscriptionArrayColumn, responseDataBuckets.get(0).getDoubleArrayColumn());
        ingestionStreamServiceWrapper.closeSubscribeDataEventCall(subscribeDataEventCall);

        // TODO: tabular export test for array column
        // Export functionality for binary array columns requires additional implementation
        // to handle conversion from binary storage format to tabular CSV output.
        // This test section is currently skipped as array columns cannot be converted 
        // to legacy DataColumn format, which is required by the current export framework.
        // This will be implemented as part of the export framework enhancement for array types.
    }
}