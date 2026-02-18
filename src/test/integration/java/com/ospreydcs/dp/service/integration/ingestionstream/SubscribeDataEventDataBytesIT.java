package com.ospreydcs.dp.service.integration.ingestionstream;

import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.ingestionstream.PvConditionTrigger;
import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventResponse;
import com.ospreydcs.dp.service.ingestionstream.IngestionStreamTestBase;
import com.ospreydcs.dp.service.integration.annotation.AnnotationIntegrationTestIntermediate;
import com.ospreydcs.dp.service.integration.ingest.GrpcIntegrationIngestionServiceWrapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class provides integration test coverage for the subscribeDataEvent() API method with scenarios that use
 * SerializedDataColumns for ingestion and the publication response streams.
 */
public class SubscribeDataEventDataBytesIT extends AnnotationIntegrationTestIntermediate {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    /**
     * This test case provides positive test coverage for a subscribeDataEvent() using SerializedDataColumns in the
     * ingestion requests so that we can confirm that SerializedDataColumns are also received in the subscription
     * response stream. It runs a simple ingestion scneario (necessary
     * before subscribing because of PV validation), then creates a subscription via subscribeDataEvent().  It then
     * runs another ingestion scenario that causes messages to be published in the subscribeDataEvent() response streams,
     * and then confirms that the messages received in the response stream are as expected.
     */
    @Test
    public void testSubscribeDataEventDataBytes() {

        final long startSeconds = Instant.now().getEpochSecond();

        {
            // Pre-populate some data in the archive for the PVs that we will be using.
            // This is necessary because validation is performed that data exists in the archive for the
            // PV names in subscribeData() requests.
            annotationIngestionScenario(startSeconds-600);
        }

        // positive subscribeDataEvent() test with coverage for SerializedDataColumns flowing from ingestion requests
        // through subscribeData() API to subscribeDataEvent() API response stream.
        // Single trigger with value = 5.0 for PV S01-BPM01.
        // Specify DataEventOperation that includes: a 3 second negative offset, a 5 second duration,
        // for target PVs S01-BPM02, S01-BPM03.
        IngestionStreamTestBase.SubscribeDataEventCall subscribeDataEventCall1;
        IngestionStreamTestBase.SubscribeDataEventRequestParams requestParams1;
        Map<PvConditionTrigger, List<SubscribeDataEventResponse.Event>> expectedEventResponses1 = new HashMap<>();
        Map<SubscribeDataEventResponse.Event, Map<String, List<Instant>>> expectedEventDataResponses1 = new HashMap<>();
        int expectedEventResponseCount1 = 0;
        int expectedDataBucketCount1;
        {
            // create list of triggers for request
            List<PvConditionTrigger> requestTriggers = new ArrayList<>();

            // create trigger
            SubscribeDataEventResponse.Event event1;
            {
                PvConditionTrigger trigger = PvConditionTrigger.newBuilder()
                        .setPvName("S01-BPM01")
                        .setCondition(PvConditionTrigger.PvCondition.PV_CONDITION_EQUAL_TO)
                        .setValue(DataValue.newBuilder().setDoubleValue(5.0).build())
                        .build();
                requestTriggers.add(trigger);

                // add entry to response verification map with trigger and expected TriggeredEvent responses
                final List<SubscribeDataEventResponse.Event> triggerExpectedEvents = new ArrayList<>();
                final DataValue eventDataValue = DataValue.newBuilder().setDoubleValue(5.0).build();
                event1 = SubscribeDataEventResponse.Event.newBuilder()
                        .setTrigger(trigger)
                        .setDataValue(eventDataValue)
                        .setEventTime(Timestamp.newBuilder().setEpochSeconds(startSeconds+5).build())
                        .build();
                triggerExpectedEvents.add(event1);
                expectedEventResponses1.put(trigger, triggerExpectedEvents);
                expectedEventResponseCount1 += triggerExpectedEvents.size();
            }

            // DataEventOperation details for params
            final String pvName1 = "S01-BPM02";
            final String pvName2 = "S01-BPM03";
            final List<String> targetPvs = List.of(pvName1, pvName2);
            final long offset = -3_000_000_000L; // 3 seconds negative trigger time offset
            final long duration = 5_000_000_000L; // 5 second duration

            // add entry for event1 to response verification map with details about expected EventData responses
            expectedDataBucketCount1 = 10;
            final List<Instant> instantList = List.of(
                    Instant.ofEpochSecond(startSeconds + 2),
                    Instant.ofEpochSecond(startSeconds + 3),
                    Instant.ofEpochSecond(startSeconds + 4),
                    Instant.ofEpochSecond(startSeconds + 5),
                    Instant.ofEpochSecond(startSeconds + 6)
            );
            final Map<String, List<Instant>> pvInstantMap = new HashMap<>();
            expectedEventDataResponses1.put(event1, pvInstantMap);
            pvInstantMap.put(pvName1, instantList);
            pvInstantMap.put(pvName2, instantList);

            // create params object (including trigger params list) for building protobuf request from params
            requestParams1 =
                    new IngestionStreamTestBase.SubscribeDataEventRequestParams(
                            requestTriggers,
                            targetPvs,
                            offset,
                            duration);

            // call subscribeDataEvent() to initiate subscription before running ingestion
            final boolean expectReject = false;
            final String expectedRejectMessage = "";
            subscribeDataEventCall1 = ingestionStreamServiceWrapper.initiateSubscribeDataEventRequest(
                    requestParams1,
                    expectedEventResponseCount1,
                    expectedDataBucketCount1,
                    expectReject,
                    expectedRejectMessage);
        }

        // run a simple ingestion scenario that that generates requests with SerializedDataColumns
        Map<String, GrpcIntegrationIngestionServiceWrapper.IngestionStreamInfo> ingestionValidationMap;
        {
            // create data for 10 sectors, each containing 3 gauges and 3 bpms
            // named with prefix "S%02d-" followed by "GCC%02d" or "BPM%02d"
            // with 10 measurements per bucket, 1 bucket per second, and 10 buckets per pv
            ingestionValidationMap = annotationIngestionScenario(startSeconds);
        }

        // request 1: verify subscribeDataEvent() responses and close request stream explicitly with onCompleted().
        ingestionStreamServiceWrapper.verifySubscribeDataEventResponse(
                (IngestionStreamTestBase.SubscribeDataEventResponseObserver) subscribeDataEventCall1.responseObserver(),
                expectedEventResponses1,
                expectedEventDataResponses1,
                expectedDataBucketCount1,
                null);
        ingestionStreamServiceWrapper.closeSubscribeDataEventCall(subscribeDataEventCall1);

    }
}
