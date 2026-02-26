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
 * This integration test covers the use of protobuf EnumColumns in the MLDP APIs.
 */
public class EnumColumnIT extends GrpcIntegrationTestBase {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    /**
     * This test case provides full MLDP API coverage for use of EnumColumns.
     * Registers a provider, which is required before ingesting data.
     * Creates a request with EnumColumns and ingests data.
     * Queries the ingested data and verifies the query result includes the EnumColumns.
     * Runs a subscription for the PVs in the EnumColumns and ingests additional data and verifies subscription receives data.
     * Runs a data event subscription for the PVs in the EnumColumns and ingests additional data and verifies subscription receives events.
     */
    @Test
    public void testEnumColumnFullApiCoverage() throws Exception {

        // register provider
        final String providerName = "enum-column-integration-test-provider";
        final String providerId = ingestionServiceWrapper.registerProvider(providerName, null);

        // create test data using EnumColumns
        final long startSeconds = Instant.now().getEpochSecond() - 100;
        final long startNanos = 123456789L;
        final long measurementInterval = 100_000_000L; // 100 ms
        int numSamples = 5;
        final List<String> pvNames = List.of("test_pv_enum_01", "test_pv_enum_02");
        {
            // create EnumColumns for ingestion
            List<EnumColumn> enumColumns = new ArrayList<>();
            for (String pvName : pvNames) {
                List<Integer> values = new ArrayList<>();
                for (int i = 0; i < numSamples; i++) {
                    values.add(i % 3); // 0 (OK), 1 (WARNING), 2 (ERROR), 0 (OK), 1 (WARNING)
                }
                EnumColumn enumColumn = EnumColumn.newBuilder()
                        .setName(pvName)
                        .addAllValues(values)
                        .setEnumId("test:alarm_status:v1")
                        .build();
                enumColumns.add(enumColumn);
            }

            // create ingestion request parameters
            IngestionTestBase.IngestionRequestParams params =
                    new IngestionTestBase.IngestionRequestParams(
                            providerId,
                            "test-request-enum-columns",
                            null,
                            null,
                            startSeconds,
                            startNanos,
                            measurementInterval,
                            numSamples,
                            pvNames,
                            IngestionTestBase.IngestionDataType.DOUBLE,
                            null,
                            null,
                            null);
            params.setEnumColumnList(enumColumns);

            // build and send ingestion request
            IngestDataRequest ingestionRequest = IngestionTestBase.buildIngestionRequest(params);
            ingestionServiceWrapper.sendAndVerifyIngestData(params, ingestionRequest);
        }

        // positive queryData() test case
        {
            // select data for query
            final long beginSeconds = startSeconds;
            final long beginNanos = startNanos;
            final long endSeconds = beginSeconds + 1L;
            final long endNanos = 0L;

            final int numBucketsExpected = pvNames.size();
            final boolean expectReject = false;
            final String expectedRejectMessage = "";

            final QueryTestBase.QueryDataRequestParams queryParams =
                    new QueryTestBase.QueryDataRequestParams(
                            pvNames,
                            beginSeconds,
                            beginNanos,
                            endSeconds,
                            endNanos
                    );

            final List<DataBucket> queryResultBuckets = queryServiceWrapper.queryData(
                    queryParams,
                    expectReject,
                    expectedRejectMessage
            );

            assertEquals(numBucketsExpected, queryResultBuckets.size());
            for (DataBucket queryResultBucket : queryResultBuckets) {
                assertEquals(DataValues.ValuesCase.ENUMCOLUMN, queryResultBucket.getDataValues().getValuesCase());
                assertTrue(pvNames.contains(queryResultBucket.getDataValues().getEnumColumn().getName()));
                assertEquals(numSamples, queryResultBucket.getDataValues().getEnumColumn().getValuesCount());
                assertEquals("test:alarm_status:v1", queryResultBucket.getDataValues().getEnumColumn().getEnumId());
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
                            pvNames, expectedResponseCount, expectReject, expectedRejectMessage);
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
                final String pvName = pvNames.get(0);
                PvConditionTrigger trigger = PvConditionTrigger.newBuilder()
                        .setPvName(pvName)
                        .setCondition(PvConditionTrigger.PvCondition.PV_CONDITION_EQUAL_TO)
                        .setValue(DataValue.newBuilder().setIntValue(2).build()) // ERROR state
                        .build();
                requestTriggers.add(trigger);

                // add entry to response verification map with trigger and expected TriggeredEvent responses
                final List<SubscribeDataEventResponse.Event> triggerExpectedEvents = new ArrayList<>();
                final DataValue eventDataValue = DataValue.newBuilder().setIntValue(2).build();
                event = SubscribeDataEventResponse.Event.newBuilder()
                        .setTrigger(trigger)
                        .setDataValue(eventDataValue)
                        .setEventTime(Timestamp.newBuilder().setEpochSeconds(startSeconds+1).setNanoseconds(startNanos).build())
                        .build();
                triggerExpectedEvents.add(event);
                expectedEventResponses.put(trigger, triggerExpectedEvents);
                expectedEventResponseCount += triggerExpectedEvents.size();
            }

            // DataEventOperation details for params
            final String pvName = pvNames.get(0);
            final List<String> targetPvs = List.of(pvName);
            final long offset = -3_000_000_000L; // 3 seconds negative trigger time offset
            final long duration = 5_000_000_000L; // 5 second duration

            // add entry for event to response verification map with details about expected EventData responses
            final int expectedDataBucketCount = 1;
            final List<Instant> instantList = List.of(Instant.ofEpochSecond(startSeconds + 1, startNanos));
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
        EnumColumn subscriptionColumn;
        {
            // positive unary ingestion test for EnumColumn
            // assemble IngestionRequest
            final String requestId = "request-9";
            final List<String> columnNames = Arrays.asList(pvNames.get(0));
            final long sampleIntervalNanos = 1_000_000L;
            numSamples = 2;

            // specify explicit EnumColumn data
            final List<EnumColumn> enumColumns = new ArrayList<>();
            EnumColumn.Builder enumColumnBuilder = EnumColumn.newBuilder();
            enumColumnBuilder.setName(pvNames.get(0));
            enumColumnBuilder.addValues(2); // ERROR state - will trigger event
            enumColumnBuilder.addValues(0); // OK state
            enumColumnBuilder.setEnumId("test:alarm_status:v1");
            subscriptionColumn = enumColumnBuilder.build();
            enumColumns.add(subscriptionColumn);

            // create request parameters
            final IngestionTestBase.IngestionRequestParams subscriptionRequestParams =
                    new IngestionTestBase.IngestionRequestParams(
                            providerId,
                            requestId,
                            null,
                            null,
                            startSeconds + 1,
                            startNanos,
                            sampleIntervalNanos,
                            numSamples,
                            columnNames,
                            null,
                            null,
                            null,
                            null
                    );
            subscriptionRequestParams.setEnumColumnList(enumColumns);

            final IngestDataRequest subscriptionRequest =
                    IngestionTestBase.buildIngestionRequest(subscriptionRequestParams);

            ingestionServiceWrapper.sendAndVerifyIngestData(subscriptionRequestParams, subscriptionRequest);
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
            assertTrue(responseBucket.getDataValues().hasEnumColumn());
            assertEquals(subscriptionColumn, responseBucket.getDataValues().getEnumColumn());
        }

        // check that expected subscribeDataEvent() responses are received
        final List<DataBucket> responseDataBuckets = ingestionStreamServiceWrapper.verifySubscribeDataEventResponse(
                (IngestionStreamTestBase.SubscribeDataEventResponseObserver) subscribeDataEventCall.responseObserver(),
                expectedEventResponses,
                expectedEventDataResponses,
                DataValues.ValuesCase.ENUMCOLUMN);
        assertEquals(1, responseDataBuckets.size());
        assertEquals(subscriptionColumn, responseDataBuckets.get(0).getDataValues().getEnumColumn());
        ingestionStreamServiceWrapper.closeSubscribeDataEventCall(subscribeDataEventCall);
    }
}