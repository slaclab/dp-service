package com.ospreydcs.dp.service.integration.ingestionstream;

import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.ingestionstream.PvConditionTrigger;
import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventResponse;
import com.ospreydcs.dp.service.ingestionstream.IngestionStreamTestBase;
import com.ospreydcs.dp.service.integration.GrpcIntegrationTestBase;
import com.ospreydcs.dp.service.integration.ingest.GrpcIntegrationIngestionServiceWrapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class provides integration test coverage for the subscribeDataEvent() API method.
 */
public class SubscribeDataEventDataIT extends GrpcIntegrationTestBase {

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
     * This test case provides positive test coverage for 3 different subscribeDataEvent() scenarios exercising details
     * of the PvConditionTrigger and DataEventOperation messages.  It runs a simple ingestion scneario (necessary
     * before subscribing because of PV validation), then creates 3 subscriptions via subscribeDataEvent().  It then
     * runs another ingestion scenario that causes messages to be published in the subscribeDataEvent() response streams,
     * and then confirms that the messages received in the response streams are as expected.
     */
    @Test
    public void testSubscribeDataEventData() {

        final long startSeconds = Instant.now().getEpochSecond();

        {
            // Pre-populate some data in the archive for the PVs that we will be using.
            // This is necessary because validation is performed that data exists in the archive for the
            // PV names in subscribeData() requests.
            ingestionServiceWrapper.simpleIngestionScenario(
                    startSeconds-600,
                    true);
        }

        // 1. request 1. positive subscribeDataEvent() test: single trigger with value = 5.0 for PV S01-BPM01
        // Specify DataEventOperation that includes: a 3 second negative offset, a 5 second duration,
        // for target PVs S01-BPM02, S01-BPM03.
        IngestionStreamTestBase.SubscribeDataEventCall subscribeDataEventCall1;
        IngestionStreamTestBase.SubscribeDataEventRequestParams requestParams1;
        Map<PvConditionTrigger, List<SubscribeDataEventResponse.Event>> expectedEventResponses1 = new HashMap<>();
        Map<SubscribeDataEventResponse.Event, Map<String, List<Instant>>> expectedEventDataResponses1 = new HashMap<>();
        int expectedEventResponseCount1 = 0;
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
            final int expectedDataBucketCount = 10;
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
                    expectedDataBucketCount,
                    expectReject,
                    expectedRejectMessage);
        }

        // 2. request 2. positive subscribeDataEvent() test: simultaneous overlapping triggered events for same subscription
        IngestionStreamTestBase.SubscribeDataEventCall subscribeDataEventCall2;
        IngestionStreamTestBase.SubscribeDataEventRequestParams requestParams2;
        Map<PvConditionTrigger, List<SubscribeDataEventResponse.Event>> expectedEventResponses2 = new HashMap<>();
        Map<SubscribeDataEventResponse.Event, Map<String, List<Instant>>> expectedEventDataResponses2 = new HashMap<>();
        int expectedEventResponseCount2 = 0;
        {
            // create list of triggers for request
            List<PvConditionTrigger> requestTriggers = new ArrayList<>();

            // create trigger
            SubscribeDataEventResponse.Event event1;
            SubscribeDataEventResponse.Event event2;
            {
                PvConditionTrigger trigger = PvConditionTrigger.newBuilder()
                        .setPvName("S02-GCC01")
                        .setCondition(PvConditionTrigger.PvCondition.PV_CONDITION_LESS)
                        .setValue(DataValue.newBuilder().setDoubleValue(0.2).build())
                        .build();
                requestTriggers.add(trigger);

                // create list of expected Events for trigger
                final List<SubscribeDataEventResponse.Event> triggerExpectedEvents = new ArrayList<>();

                // create TriggeredEvent and add to list, data value 0.0
                {
                    event1 = SubscribeDataEventResponse.Event.newBuilder()
                            .setTrigger(trigger)
                            .setDataValue(DataValue.newBuilder().setDoubleValue(0).build())
                            .setEventTime(Timestamp.newBuilder().setEpochSeconds(startSeconds).build())
                            .build();
                    triggerExpectedEvents.add(event1);
                }

                // create TriggeredEvent and add to list, data value 0.1
                {
                    event2 = SubscribeDataEventResponse.Event.newBuilder()
                            .setTrigger(trigger)
                            .setDataValue(DataValue.newBuilder().setDoubleValue(0.1).build())
                            .setEventTime(Timestamp.newBuilder().setEpochSeconds(startSeconds).setNanoseconds(100000000).build())
                            .build();
                    triggerExpectedEvents.add(event2);
                }

                // add entry to response validation map with trigger and list of expected events
                expectedEventResponses2.put(trigger, triggerExpectedEvents);
                expectedEventResponseCount2 += triggerExpectedEvents.size();
            }

            // DataEventOperation details for params
            final String pvName1 = "S01-BPM02";
            final String pvName2 = "S01-BPM03";
            final List<String> targetPvs = List.of(pvName1, pvName2);
            final long offset = 0L; // 3 seconds negative trigger time offset
            final long duration = 5_000_000_000L; // 5 second duration

            // number of buckets expected in EventData responses
            final int expectedDataBucketCount = 22;

            // add entry for event1 to response verification map with details about expected EventData responses
            final List<Instant> instantList1 = List.of(
                    Instant.ofEpochSecond(startSeconds + 0),
                    Instant.ofEpochSecond(startSeconds + 1),
                    Instant.ofEpochSecond(startSeconds + 2),
                    Instant.ofEpochSecond(startSeconds + 3),
                    Instant.ofEpochSecond(startSeconds + 4)
            );
            final Map<String, List<Instant>> pvInstantMap1 = new HashMap<>();
            expectedEventDataResponses2.put(event1, pvInstantMap1);
            pvInstantMap1.put(pvName1, instantList1);
            pvInstantMap1.put(pvName2, instantList1);

            // add entry for event2 to verification map
            final List<Instant> instantList2 = List.of(
                    Instant.ofEpochSecond(startSeconds + 0),
                    Instant.ofEpochSecond(startSeconds + 1),
                    Instant.ofEpochSecond(startSeconds + 2),
                    Instant.ofEpochSecond(startSeconds + 3),
                    Instant.ofEpochSecond(startSeconds + 4),
                    Instant.ofEpochSecond(startSeconds + 5)
            );
            final Map<String, List<Instant>> pvInstantMap2 = new HashMap<>();
            expectedEventDataResponses2.put(event2, pvInstantMap2);
            pvInstantMap2.put(pvName1, instantList2);
            pvInstantMap2.put(pvName2, instantList2);

            // create params object (including trigger params list) for building protobuf request from params
            requestParams2 =
                    new IngestionStreamTestBase.SubscribeDataEventRequestParams(
                            requestTriggers,
                            targetPvs,
                            offset,
                            duration);

            // call subscribeDataEvent() to initiate subscription before running ingestion
            final boolean expectReject = false;
            final String expectedRejectMessage = "";
            subscribeDataEventCall2 =
                    ingestionStreamServiceWrapper.initiateSubscribeDataEventRequest(
                            requestParams2,
                            expectedEventResponseCount2,
                            expectedDataBucketCount,
                            expectReject,
                            expectedRejectMessage);
        }

        // 3. request 3. positive subscribeDataEvent() test: single trigger with value = 2.5 for PV S03-BPM01
        // Specify DataEventOperation that includes: a 2 second positive offset, a 3 second duration,
        // for target PVs S03-BPM01, S03-BPM02, S03-BPM03.  Note that S03-BPM01 is being used as both a trigger and
        // target PV in the subscription.
        IngestionStreamTestBase.SubscribeDataEventCall subscribeDataEventCall3;
        IngestionStreamTestBase.SubscribeDataEventRequestParams requestParams3;
        Map<PvConditionTrigger, List<SubscribeDataEventResponse.Event>> expectedEventResponses3 = new HashMap<>();
        Map<SubscribeDataEventResponse.Event, Map<String, List<Instant>>> expectedEventDataResponses3 = new HashMap<>();
        int expectedEventResponseCount3 = 0;
        {
            // create list of triggers for request
            List<PvConditionTrigger> requestTriggers = new ArrayList<>();

            // create trigger
            SubscribeDataEventResponse.Event event1;
            {
                PvConditionTrigger trigger = PvConditionTrigger.newBuilder()
                        .setPvName("S03-BPM01")
                        .setCondition(PvConditionTrigger.PvCondition.PV_CONDITION_EQUAL_TO)
                        .setValue(DataValue.newBuilder().setDoubleValue(2.5).build())
                        .build();
                requestTriggers.add(trigger);

                // add entry to response verification map with trigger and expected TriggeredEvent responses
                final List<SubscribeDataEventResponse.Event> triggerExpectedEvents = new ArrayList<>();
                final DataValue eventDataValue = DataValue.newBuilder().setDoubleValue(2.5).build();
                event1 = SubscribeDataEventResponse.Event.newBuilder()
                        .setTrigger(trigger)
                        .setDataValue(eventDataValue)
                        .setEventTime(
                                Timestamp.newBuilder().setEpochSeconds(startSeconds+2).setNanoseconds(500000000).build())
                        .build();
                triggerExpectedEvents.add(event1);
                expectedEventResponses3.put(trigger, triggerExpectedEvents);
                expectedEventResponseCount3 += triggerExpectedEvents.size();
            }

            // DataEventOperation details for params
            final String pvName1 = "S03-BPM01";
            final String pvName2 = "S03-BPM02";
            final String pvName3 = "S03-BPM03";
            final List<String> targetPvs = List.of(pvName1, pvName2, pvName3);
            final long offset = 2_000_000_000L; // 2 seconds positive trigger time offset
            final long duration = 3_000_000_000L; // 3 second duration

            // total number of data buckets expected
            final int expectedDataBucketCount = 12;

            // add entry for event1 to response verification map with details about expected EventData responses
            final List<Instant> instantList = List.of(
                    Instant.ofEpochSecond(startSeconds + 4),
                    Instant.ofEpochSecond(startSeconds + 5),
                    Instant.ofEpochSecond(startSeconds + 6),
                    Instant.ofEpochSecond(startSeconds + 7)
            );
            final Map<String, List<Instant>> pvInstantMap = new HashMap<>();
            expectedEventDataResponses3.put(event1, pvInstantMap);
            pvInstantMap.put(pvName1, instantList);
            pvInstantMap.put(pvName2, instantList);
            pvInstantMap.put(pvName3, instantList);

            // create params object (including trigger params list) for building protobuf request from params
            requestParams3 =
                    new IngestionStreamTestBase.SubscribeDataEventRequestParams(
                            requestTriggers,
                            targetPvs,
                            offset,
                            duration);

            // call subscribeDataEvent() to initiate subscription before running ingestion
            final boolean expectReject = false;
            final String expectedRejectMessage = "";
            subscribeDataEventCall3 = ingestionStreamServiceWrapper.initiateSubscribeDataEventRequest(
                    requestParams3,
                    expectedEventResponseCount3,
                    expectedDataBucketCount,
                    expectReject,
                    expectedRejectMessage);
        }

        // run a simple ingestion scenario that will publish data relevant to subscriptions
        GrpcIntegrationIngestionServiceWrapper.IngestionScenarioResult ingestionScenarioResult;
        {
            // create some data for testing query APIs
            // create data for 10 sectors, each containing 3 gauges and 3 bpms
            // named with prefix "S%02d-" followed by "GCC%02d" or "BPM%02d"
            // with 10 measurements per bucket, 1 bucket per second, and 10 buckets per pv
            ingestionScenarioResult = ingestionServiceWrapper.simpleIngestionScenario(startSeconds, false);
        }

        // request 1: verify subscribeDataEvent() responses and close request stream explicitly with onCompleted().
        ingestionStreamServiceWrapper.verifySubscribeDataEventResponse(
                (IngestionStreamTestBase.SubscribeDataEventResponseObserver) subscribeDataEventCall1.responseObserver(),
                expectedEventResponses1,
                expectedEventDataResponses1,
                0,
                null);
        ingestionStreamServiceWrapper.closeSubscribeDataEventCall(subscribeDataEventCall1);

        // request 2: verify subscribeDataEvent() responses and close request stream explicitly with cancel request.
        ingestionStreamServiceWrapper.verifySubscribeDataEventResponse(
                (IngestionStreamTestBase.SubscribeDataEventResponseObserver) subscribeDataEventCall2.responseObserver(),
                expectedEventResponses2,
                expectedEventDataResponses2,
                0,
                null);
        ingestionStreamServiceWrapper.cancelSubscribeDataEventCall(subscribeDataEventCall2);

        // request 3: verify subscribeDataEvent() responses and let server close API stream implicitly
        ingestionStreamServiceWrapper.verifySubscribeDataEventResponse(
                (IngestionStreamTestBase.SubscribeDataEventResponseObserver) subscribeDataEventCall3.responseObserver(),
                expectedEventResponses3,
                expectedEventDataResponses3,
                0,
                null);
//        subscribeDataEventCall3.requestObserver().onCompleted();

    }

    /**
     * This test case provides negative test coverage for various scenarios that cause the subscribeDataEvent() request
     * to be rejectd.  The individual scenarios are described inline.
     */
    @Test
    public void testSubscribeDataEventDataReject() {

        final long startSeconds = Instant.now().getEpochSecond();

        // negative subscribeDataEvent() test: rejected because operation list of target PV names is empty
        {
            IngestionStreamTestBase.SubscribeDataEventCall subscribeDataEventCall;
            IngestionStreamTestBase.SubscribeDataEventRequestParams requestParams;
            Map<PvConditionTrigger, List<SubscribeDataEventResponse.Event>> expectedEventResponses = new HashMap<>();
            Map<SubscribeDataEventResponse.Event, Map<String, List<Instant>>> expectedEventDataResponses = new HashMap<>();
            int expectedEventResponseCount = 0;

            // create list of triggers for request
            List<PvConditionTrigger> requestTriggers = new ArrayList<>();

            // create trigger
            SubscribeDataEventResponse.Event event1;
            {
                PvConditionTrigger trigger = PvConditionTrigger.newBuilder()
                        .setPvName("S03-BPM01")
                        .setCondition(PvConditionTrigger.PvCondition.PV_CONDITION_EQUAL_TO)
                        .setValue(DataValue.newBuilder().setDoubleValue(2.5).build())
                        .build();
                requestTriggers.add(trigger);

                // add entry to response verification map with trigger and expected TriggeredEvent responses
                final List<SubscribeDataEventResponse.Event> triggerExpectedEvents = new ArrayList<>();
                final DataValue eventDataValue = DataValue.newBuilder().setDoubleValue(2.5).build();
                event1 = SubscribeDataEventResponse.Event.newBuilder()
                        .setTrigger(trigger)
                        .setDataValue(eventDataValue)
                        .setEventTime(
                                Timestamp.newBuilder().setEpochSeconds(startSeconds+2).setNanoseconds(500000000).build())
                        .build();
                triggerExpectedEvents.add(event1);
                expectedEventResponses.put(trigger, triggerExpectedEvents);
                expectedEventResponseCount += triggerExpectedEvents.size();
            }

            // DataEventOperation details for params
//            final String pvName1 = "S03-BPM02";
//            final String pvName2 = "S03-BPM03";
            final List<String> targetPvs = new ArrayList<>(); // EMPTY LIST OF TARGET PV NAMES CAUSES REJECT
            final long offset = 2_000_000_000L; // 2 seconds positive trigger time offset
            final long duration = 3_000_000_000L; // 3 second duration

            // total number of data buckets expected
            final int expectedDataBucketCount = 8;

            // create params object (including trigger params list) for building protobuf request from params
            requestParams =
                    new IngestionStreamTestBase.SubscribeDataEventRequestParams(
                            requestTriggers,
                            targetPvs,
                            offset,
                            duration);

            // call subscribeDataEvent() to initiate subscription before running ingestion
            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "SubscribeDataEventRequest DataEventOperation.targetPvs list must not be empty";
            subscribeDataEventCall = ingestionStreamServiceWrapper.initiateSubscribeDataEventRequest(
                    requestParams,
                    expectedEventResponseCount,
                    expectedDataBucketCount,
                    expectReject,
                    expectedRejectMessage);
        }

        // negative subscribeDataEvent() test: rejected because operation list of target PV names contains a blank name
        {
            IngestionStreamTestBase.SubscribeDataEventCall subscribeDataEventCall;
            IngestionStreamTestBase.SubscribeDataEventRequestParams requestParams;
            Map<PvConditionTrigger, List<SubscribeDataEventResponse.Event>> expectedEventResponses = new HashMap<>();
            Map<SubscribeDataEventResponse.Event, Map<String, List<Instant>>> expectedEventDataResponses = new HashMap<>();
            int expectedEventResponseCount = 0;

            // create list of triggers for request
            List<PvConditionTrigger> requestTriggers = new ArrayList<>();

            // create trigger
            SubscribeDataEventResponse.Event event1;
            {
                PvConditionTrigger trigger = PvConditionTrigger.newBuilder()
                        .setPvName("S03-BPM01")
                        .setCondition(PvConditionTrigger.PvCondition.PV_CONDITION_EQUAL_TO)
                        .setValue(DataValue.newBuilder().setDoubleValue(2.5).build())
                        .build();
                requestTriggers.add(trigger);

                // add entry to response verification map with trigger and expected TriggeredEvent responses
                final List<SubscribeDataEventResponse.Event> triggerExpectedEvents = new ArrayList<>();
                final DataValue eventDataValue = DataValue.newBuilder().setDoubleValue(2.5).build();
                event1 = SubscribeDataEventResponse.Event.newBuilder()
                        .setTrigger(trigger)
                        .setDataValue(eventDataValue)
                        .setEventTime(
                                Timestamp.newBuilder().setEpochSeconds(startSeconds+2).setNanoseconds(500000000).build())
                        .build();
                triggerExpectedEvents.add(event1);
                expectedEventResponses.put(trigger, triggerExpectedEvents);
                expectedEventResponseCount += triggerExpectedEvents.size();
            }

            // DataEventOperation details for params
            final String pvName1 = "S03-BPM02";
            final String pvName2 = "    "; // BLANK PV NAME CAUSES REJECT
            final List<String> targetPvs = List.of(pvName1, pvName2);
            final long offset = 2_000_000_000L; // 2 seconds positive trigger time offset
            final long duration = 3_000_000_000L; // 3 second duration

            // total number of data buckets expected
            final int expectedDataBucketCount = 8;

            // create params object (including trigger params list) for building protobuf request from params
            requestParams =
                    new IngestionStreamTestBase.SubscribeDataEventRequestParams(
                            requestTriggers,
                            targetPvs,
                            offset,
                            duration);

            // call subscribeDataEvent() to initiate subscription before running ingestion
            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "SubscribeDataEventRequest DataEventOperation.targetPvs contains empty string";
            subscribeDataEventCall = ingestionStreamServiceWrapper.initiateSubscribeDataEventRequest(
                    requestParams,
                    expectedEventResponseCount,
                    expectedDataBucketCount,
                    expectReject,
                    expectedRejectMessage);
        }

        // negative subscribeDataEvent() test: rejected because DataEventOperation.window not specified
        {
            IngestionStreamTestBase.SubscribeDataEventCall subscribeDataEventCall;
            IngestionStreamTestBase.SubscribeDataEventRequestParams requestParams;
            Map<PvConditionTrigger, List<SubscribeDataEventResponse.Event>> expectedEventResponses = new HashMap<>();
            Map<SubscribeDataEventResponse.Event, Map<String, List<Instant>>> expectedEventDataResponses = new HashMap<>();
            int expectedEventResponseCount = 0;

            // create list of triggers for request
            List<PvConditionTrigger> requestTriggers = new ArrayList<>();

            // create trigger
            SubscribeDataEventResponse.Event event1;
            {
                PvConditionTrigger trigger = PvConditionTrigger.newBuilder()
                        .setPvName("S03-BPM01")
                        .setCondition(PvConditionTrigger.PvCondition.PV_CONDITION_EQUAL_TO)
                        .setValue(DataValue.newBuilder().setDoubleValue(2.5).build())
                        .build();
                requestTriggers.add(trigger);

                // add entry to response verification map with trigger and expected TriggeredEvent responses
                final List<SubscribeDataEventResponse.Event> triggerExpectedEvents = new ArrayList<>();
                final DataValue eventDataValue = DataValue.newBuilder().setDoubleValue(2.5).build();
                event1 = SubscribeDataEventResponse.Event.newBuilder()
                        .setTrigger(trigger)
                        .setDataValue(eventDataValue)
                        .setEventTime(
                                Timestamp.newBuilder().setEpochSeconds(startSeconds+2).setNanoseconds(500000000).build())
                        .build();
                triggerExpectedEvents.add(event1);
                expectedEventResponses.put(trigger, triggerExpectedEvents);
                expectedEventResponseCount += triggerExpectedEvents.size();
            }

            // DataEventOperation details for params
            final String pvName1 = "S03-BPM02";
            final String pvName2 = "S03-BPM03";
            final List<String> targetPvs = List.of(pvName1, pvName2);
            final long offset = 2_000_000_000L; // 2 seconds positive trigger time offset
            final long duration = 3_000_000_000L; // 3 second duration

            // total number of data buckets expected
            final int expectedDataBucketCount = 8;

            // create params object (including trigger params list) for building protobuf request from params
            requestParams =
                    new IngestionStreamTestBase.SubscribeDataEventRequestParams(
                            requestTriggers,
                            targetPvs,
                            offset,
                            duration);
            requestParams.noWindow = true; // BUILDS REQUEST WITHOUT DATAEVENTWINDOW, WHICH CAUSES REJECT

            // call subscribeDataEvent() to initiate subscription before running ingestion
            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "SubscribeDataEventRequest DataEventOperation.window must be specified";
            subscribeDataEventCall = ingestionStreamServiceWrapper.initiateSubscribeDataEventRequest(
                    requestParams,
                    expectedEventResponseCount,
                    expectedDataBucketCount,
                    expectReject,
                    expectedRejectMessage);
        }

        // negative subscribeDataEvent() test: rejected because window time interval duration is zero
        {
            IngestionStreamTestBase.SubscribeDataEventCall subscribeDataEventCall;
            IngestionStreamTestBase.SubscribeDataEventRequestParams requestParams;
            Map<PvConditionTrigger, List<SubscribeDataEventResponse.Event>> expectedEventResponses = new HashMap<>();
            Map<SubscribeDataEventResponse.Event, Map<String, List<Instant>>> expectedEventDataResponses = new HashMap<>();
            int expectedEventResponseCount = 0;

            // create list of triggers for request
            List<PvConditionTrigger> requestTriggers = new ArrayList<>();

            // create trigger
            SubscribeDataEventResponse.Event event1;
            {
                PvConditionTrigger trigger = PvConditionTrigger.newBuilder()
                        .setPvName("S03-BPM01")
                        .setCondition(PvConditionTrigger.PvCondition.PV_CONDITION_EQUAL_TO)
                        .setValue(DataValue.newBuilder().setDoubleValue(2.5).build())
                        .build();
                requestTriggers.add(trigger);

                // add entry to response verification map with trigger and expected TriggeredEvent responses
                final List<SubscribeDataEventResponse.Event> triggerExpectedEvents = new ArrayList<>();
                final DataValue eventDataValue = DataValue.newBuilder().setDoubleValue(2.5).build();
                event1 = SubscribeDataEventResponse.Event.newBuilder()
                        .setTrigger(trigger)
                        .setDataValue(eventDataValue)
                        .setEventTime(
                                Timestamp.newBuilder().setEpochSeconds(startSeconds+2).setNanoseconds(500000000).build())
                        .build();
                triggerExpectedEvents.add(event1);
                expectedEventResponses.put(trigger, triggerExpectedEvents);
                expectedEventResponseCount += triggerExpectedEvents.size();
            }

            // DataEventOperation details for params
            final String pvName1 = "S03-BPM02";
            final String pvName2 = "S03-BPM03"; // BLANK PV NAME CAUSES REJECT
            final List<String> targetPvs = List.of(pvName1, pvName2);
            final long offset = 2_000_000_000L; // 2 seconds positive trigger time offset
            final long duration = 0L; // 3 second duration

            // total number of data buckets expected
            final int expectedDataBucketCount = 8;

            // create params object (including trigger params list) for building protobuf request from params
            requestParams =
                    new IngestionStreamTestBase.SubscribeDataEventRequestParams(
                            requestTriggers,
                            targetPvs,
                            offset,
                            duration);

            // call subscribeDataEvent() to initiate subscription before running ingestion
            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "SubscribeDataEventRequest.DataEventOperation.DataEventWindow TimeInterval.duration must be specified";
            subscribeDataEventCall = ingestionStreamServiceWrapper.initiateSubscribeDataEventRequest(
                    requestParams,
                    expectedEventResponseCount,
                    expectedDataBucketCount,
                    expectReject,
                    expectedRejectMessage);
        }

    }

}
