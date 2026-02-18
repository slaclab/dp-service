package com.ospreydcs.dp.service.integration.v2api;

import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.DoubleColumn;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
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
 * This integration test covers the use of protobuf DoubleColumns in the MLDP APIs.
 */
public class DoubleColumnIT extends GrpcIntegrationTestBase {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    /**
     * This test case provides full MLDP API coverage for use of DoubleColumns.
     * Registers a provider, which is required before
     * using the ingestion APIs.  Uses the data ingestion API to send an IngestDataRequest whose IngestionDataFrame
     * contains a DoubleColumn data structure.  Uses the time-series data query API to retrieve the bucket containing
     * the DataColumn sent in the ingestion request.  Confirms that the DoubleColumn retrieved via the query API matches
     * the column sent in the ingestion request, using DataColumn.equals() which compares column name and data values
     * in the two columns.  Subscribes for PV data via the subscribeData() API method and confirms that the data
     * received in the subscription response stream matches the ingested data.  Registers via subscribeDataEvent() for
     * data events and confirms that the appropriate responses are received.
     */
    @Test
    public void doubleColumnTest() {

        String providerId;
        {
            // register ingestion provider
            final String providerName = String.valueOf(1);
            providerId = ingestionServiceWrapper.registerProvider(providerName, null);
        }

        final String pvName = "pv_08";
        List<String> columnNames = Arrays.asList(pvName);
        long firstSeconds = Instant.now().getEpochSecond();
        long firstNanos = 0L;
        IngestionTestBase.IngestionRequestParams ingestionRequestParams;
        DoubleColumn requestDoubleColumn;
        {
            // positive unary ingestion test for DoubleColumn
            // assemble IngestionRequest
            final String requestId = "request-8";
            final long sampleIntervalNanos = 1_000_000L;
            final int numSamples = 2;

            // specify explicit DoubleColumn data
            final List<DoubleColumn> doubleColumns = new ArrayList<>();
            DoubleColumn.Builder doubleColumnBuilder = DoubleColumn.newBuilder();
            doubleColumnBuilder.setName(pvName);
            doubleColumnBuilder.addValues(12.34);
            doubleColumnBuilder.addValues(34.56);
            requestDoubleColumn = doubleColumnBuilder.build();
            doubleColumns.add(requestDoubleColumn);

            // create request parameters
            ingestionRequestParams =
                    new IngestionTestBase.IngestionRequestParams(
                            providerId,
                            requestId,
                            null,
                            null,
                            firstSeconds,
                            firstNanos,
                            sampleIntervalNanos,
                            numSamples,
                            columnNames,
                            null,
                            null,
                            null,
                            false,
                            null
                    );
            ingestionRequestParams.setDoubleColumnList(doubleColumns); // add list of DoubleColumns to request parameters

            final IngestDataRequest request =
                    IngestionTestBase.buildIngestionRequest(ingestionRequestParams);

            ingestionServiceWrapper.sendAndVerifyIngestData(ingestionRequestParams, request, 0);
        }

        // positive queryData() test case
        {
            // select 5 seconds of data for each pv
            final long beginSeconds = firstSeconds;
            final long beginNanos = firstNanos;
            final long endSeconds = beginSeconds + 1L;
            final long endNanos = 0L;

            // 2 pvs, 5 seconds, 1 bucket per second per pv
            final int numBucketsExpected = 1;
            final boolean expectReject = false;
            final String expectedRejectMessage = "";

            final QueryTestBase.QueryDataRequestParams params =
                    new QueryTestBase.QueryDataRequestParams(
                            columnNames,
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
                assertEquals(requestDoubleColumn, queryResultBucket.getDoubleColumn());
            }
        }

        // create a data subscription, verification succeeds because data have been ingested for the subscription PV
        SubscribeDataUtility.SubscribeDataCall subscribeDataCall;
        {
            final int expectedResponseCount = 1;
            final boolean expectReject = false;
            final String expectedRejectMessage = "";
            subscribeDataCall =
                    ingestionServiceWrapper.initiateSubscribeDataRequest(
                            columnNames, expectedResponseCount, expectReject, expectedRejectMessage);
        }

        // create a data event subscription
        IngestionStreamTestBase.SubscribeDataEventCall subscribeDataEventCall;
        IngestionStreamTestBase.SubscribeDataEventRequestParams requestParams;
        Map<PvConditionTrigger, List<SubscribeDataEventResponse.Event>> expectedEventResponses = new HashMap<>();
        Map<SubscribeDataEventResponse.Event, Map<String, List<Instant>>> expectedEventDataResponses = new HashMap<>();
        int expectedEventResponseCount = 0;
        {
            // create list of triggers for request
            List<PvConditionTrigger> requestTriggers = new ArrayList<>();

            // create trigger
            SubscribeDataEventResponse.Event event;
            {
                PvConditionTrigger trigger = PvConditionTrigger.newBuilder()
                        .setPvName(pvName)
                        .setCondition(PvConditionTrigger.PvCondition.PV_CONDITION_EQUAL_TO)
                        .setValue(DataValue.newBuilder().setDoubleValue(98.76).build())
                        .build();
                requestTriggers.add(trigger);

                // add entry to response verification map with trigger and expected TriggeredEvent responses
                final List<SubscribeDataEventResponse.Event> triggerExpectedEvents = new ArrayList<>();
                final DataValue eventDataValue = DataValue.newBuilder().setDoubleValue(98.76).build();
                event = SubscribeDataEventResponse.Event.newBuilder()
                        .setTrigger(trigger)
                        .setDataValue(eventDataValue)
                        .setEventTime(Timestamp.newBuilder().setEpochSeconds(firstSeconds+1).build())
                        .build();
                triggerExpectedEvents.add(event);
                expectedEventResponses.put(trigger, triggerExpectedEvents);
                expectedEventResponseCount += triggerExpectedEvents.size();
            }

            // DataEventOperation details for params
            final List<String> targetPvs = List.of(pvName);
            final long offset = -3_000_000_000L; // 3 seconds negative trigger time offset
            final long duration = 5_000_000_000L; // 5 second duration

            // add entry for event to response verification map with details about expected EventData responses
            final int expectedDataBucketCount = 1;
            final List<Instant> instantList = List.of(Instant.ofEpochSecond(firstSeconds + 1));
            final Map<String, List<Instant>> pvInstantMap = new HashMap<>();
            expectedEventDataResponses.put(event, pvInstantMap);
            pvInstantMap.put(pvName, instantList);

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

        // ingest data that will be published data subscription and trigger data event subscription
        DoubleColumn subscriptionColumn;
        {
            // positive unary ingestion test for DoubleColumn
            // assemble IngestionRequest
            final String requestId = "request-9";
            columnNames = Arrays.asList(pvName);
            final long sampleIntervalNanos = 1_000_000L;
            final int numSamples = 2;

            // specify explicit DoubleColumn data
            final List<DoubleColumn> doubleColumns = new ArrayList<>();
            DoubleColumn.Builder doubleColumnBuilder = DoubleColumn.newBuilder();
            doubleColumnBuilder.setName(pvName);
            doubleColumnBuilder.addValues(98.76);
            doubleColumnBuilder.addValues(54.32);
            subscriptionColumn = doubleColumnBuilder.build();
            doubleColumns.add(subscriptionColumn);

            // create request parameters
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
                            columnNames,
                            null,
                            null,
                            null,
                            false,
                            null
                    );
            subscriptionRequestParams.setDoubleColumnList(doubleColumns); // add list of DoubleColumns to request parameters

            final IngestDataRequest request =
                    IngestionTestBase.buildIngestionRequest(subscriptionRequestParams);

            ingestionServiceWrapper.sendAndVerifyIngestData(subscriptionRequestParams, request, 0);
        }

        // check that expected subscribeData() response is received
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
            assertTrue(responseBucket.hasDoubleColumn());
            assertEquals(subscriptionColumn, responseBucket.getDoubleColumn());
        }

        // check that expected subscribeDataEvent() responses are received
        final List<DataBucket> responseDataBuckets = ingestionStreamServiceWrapper.verifySubscribeDataEventResponse(
                (IngestionStreamTestBase.SubscribeDataEventResponseObserver) subscribeDataEventCall.responseObserver(),
                expectedEventResponses,
                expectedEventDataResponses,
                0,
                DataBucket.DataCase.DOUBLECOLUMN);
        assertEquals(1, responseDataBuckets.size());
        assertEquals(subscriptionColumn, responseDataBuckets.get(0).getDoubleColumn());
        ingestionStreamServiceWrapper.closeSubscribeDataEventCall(subscribeDataEventCall);
    }

}
