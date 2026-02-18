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

import static org.junit.Assert.assertTrue;

/**
 * This class provides integration test coverage for the event triggering mechanism of the subscribeDataEvent()
 * handling framework.
 */
public class SubscribeDataEventTriggerIT extends GrpcIntegrationTestBase {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        logger.debug("SubscribeDataEventTriggerIT tearDown");
        super.tearDown();
    }

    /**
     * This test case provides positive test coverage for subscribeDataEvent() event triggering.  It ingests some data
     * for the relevant PVs (required so that subscription PV validation succeeds), then creates 3 subscriptions via
     * subscribeDataEvent() that exercise the event triggering framework (see comments for scenario details inline). It
     * then runs another ingestion scenario that causes triggered event messages to be sent in the subscription response
     * streams, and verifies the content of those messages.
     */
    @Test
    public void testSubscribeDataEventTrigger() {

        final long startSeconds = Instant.now().getEpochSecond();

        {
            // Pre-populate some data in the archive for the PVs that we will be using.
            // This is necessary because validation is performed that data exists in the archive for the
            // PV names in subscribeData() requests.
            ingestionServiceWrapper.simpleIngestionScenario(
                    startSeconds-600,
                    true);
        }

        {
            // 1. request 1. positive subscribeDataEvent() test: single trigger with value = 5.0 for PV S01-BPM01
            IngestionStreamTestBase.SubscribeDataEventCall subscribeDataEventCall;
            IngestionStreamTestBase.SubscribeDataEventRequestParams requestParams1;
            Map<PvConditionTrigger, List<SubscribeDataEventResponse.Event>> expectedEventResponses1 = new HashMap<>();
            int expectedResponseCount1 = 0;
            {
                // create list of triggers for request
                List<PvConditionTrigger> requestTriggers = new ArrayList<>();

                // create trigger, add entry to response verification map with trigger and expected Events
                {
                    PvConditionTrigger trigger = PvConditionTrigger.newBuilder()
                            .setPvName("S01-BPM01")
                            .setCondition(PvConditionTrigger.PvCondition.PV_CONDITION_EQUAL_TO)
                            .setValue(DataValue.newBuilder().setDoubleValue(5.0).build())
                            .build();
                    requestTriggers.add(trigger);
                    final List<SubscribeDataEventResponse.Event> triggerExpectedEvents = new ArrayList<>();
                    final DataValue eventDataValue = DataValue.newBuilder().setDoubleValue(5.0).build();
                    final SubscribeDataEventResponse.Event event = SubscribeDataEventResponse.Event.newBuilder()
                            .setTrigger(trigger)
                            .setDataValue(eventDataValue)
                            .setEventTime(Timestamp.newBuilder().setEpochSeconds(startSeconds+5).build())
                            .build();
                    triggerExpectedEvents.add(event);
                    expectedEventResponses1.put(trigger, triggerExpectedEvents);
                    expectedResponseCount1 += triggerExpectedEvents.size();
                }

                // create params object (including trigger params list) for building protobuf request from params
                requestParams1 =
                        new IngestionStreamTestBase.SubscribeDataEventRequestParams(requestTriggers, null, null, null);

                // call subscribeDataEvent() to initiate subscription before running ingestion
                final boolean expectReject = false;
                final String expectedRejectMessage = "";
                subscribeDataEventCall =
                        ingestionStreamServiceWrapper.initiateSubscribeDataEventRequest(
                                requestParams1,
                                expectedResponseCount1,
                                0, expectReject,
                                expectedRejectMessage);
            }

            // request 2. positive subscribeDataEvent() test: two triggers:
            // trigger 1: pv S02-GCC-01, trigger condition less than, trigger value 0.2
            // trigger 2: pv S02-GCC-02, trigger condition greater than, trigger value 9.8
            IngestionStreamTestBase.SubscribeDataEventCall subscribeDataEventCall2;
            IngestionStreamTestBase.SubscribeDataEventRequestParams requestParams2;
            Map<PvConditionTrigger, List<SubscribeDataEventResponse.Event>> expectedEventResponses2 = new HashMap<>();
            int expectedResponseCount2 = 0;
            {
                // create list of triggers for request
                List<PvConditionTrigger> requestTriggers = new ArrayList<>();

                // create trigger 1 and add entry to map with trigger and corresponding list of expected Events
                // for response verification
                {
                    // create trigger and add to request trigger list
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
                        final SubscribeDataEventResponse.Event event = SubscribeDataEventResponse.Event.newBuilder()
                                .setTrigger(trigger)
                                .setDataValue(DataValue.newBuilder().setDoubleValue(0).build())
                                .setEventTime(Timestamp.newBuilder().setEpochSeconds(startSeconds).build())
                                .build();
                        triggerExpectedEvents.add(event);
                    }

                    // create TriggeredEvent and add to list, data value 0.1
                    {
                        final SubscribeDataEventResponse.Event event = SubscribeDataEventResponse.Event.newBuilder()
                                .setTrigger(trigger)
                                .setDataValue(DataValue.newBuilder().setDoubleValue(0.1).build())
                                .setEventTime(Timestamp.newBuilder().setEpochSeconds(startSeconds).setNanoseconds(100000000).build())
                                .build();
                        triggerExpectedEvents.add(event);
                    }

                    // add entry to response validation map with trigger and list of expected events
                    expectedEventResponses2.put(trigger, triggerExpectedEvents);
                    expectedResponseCount2 += triggerExpectedEvents.size();
                }

                // create trigger 2 and add entry to map with trigger and corresponding list of expected Events
                // for response verification
                {
                    // create trigger and add to request trigger list
                    PvConditionTrigger trigger = PvConditionTrigger.newBuilder()
                            .setPvName("S02-GCC02")
                            .setCondition(PvConditionTrigger.PvCondition.PV_CONDITION_GREATER)
                            .setValue(DataValue.newBuilder().setDoubleValue(9.8).build())
                            .build();
                    requestTriggers.add(trigger);

                    // create list of expected Events for trigger
                    final List<SubscribeDataEventResponse.Event> triggerExpectedEvents = new ArrayList<>();

                    // create TriggeredEvent and add to list, data value 9.9
                    {
                        final SubscribeDataEventResponse.Event event = SubscribeDataEventResponse.Event.newBuilder()
                                .setTrigger(trigger)
                                .setDataValue(DataValue.newBuilder().setDoubleValue(9.9).build())
                                .setEventTime(Timestamp.newBuilder().setEpochSeconds(startSeconds+9).setNanoseconds(900000000).build())
                                .build();
                        triggerExpectedEvents.add(event);
                    }

                    // add entry to response validation map with trigger and list of expected events
                    expectedEventResponses2.put(trigger, triggerExpectedEvents);
                    expectedResponseCount2 += triggerExpectedEvents.size();
                }

                // create params object (including trigger params list) for building protobuf request from params
                requestParams2 =
                        new IngestionStreamTestBase.SubscribeDataEventRequestParams(requestTriggers, null, null, null);

                // call subscribeDataEvent() to initiate subscription before running ingestion
                final boolean expectReject = false;
                final String expectedRejectMessage = "";
                subscribeDataEventCall2 =
                        ingestionStreamServiceWrapper.initiateSubscribeDataEventRequest(
                                requestParams2,
                                expectedResponseCount2,
                                0, expectReject,
                                expectedRejectMessage);
            }

            // request 3. positive subscribeDataEvent() test: two triggers:
            // trigger 1: pv S02-GCC-01, trigger condition less than or equal, trigger value 0.2
            // trigger 2: pv S02-GCC-02, trigger condition greater than or equal, trigger value 9.8
            IngestionStreamTestBase.SubscribeDataEventCall subscribeDataEventCall3;
            IngestionStreamTestBase.SubscribeDataEventRequestParams requestParams3;
            Map<PvConditionTrigger, List<SubscribeDataEventResponse.Event>> expectedEventResponses3 = new HashMap<>();
            int expectedResponseCount3 = 0;
            {
                // create list of triggers for request
                List<PvConditionTrigger> requestTriggers = new ArrayList<>();

                // create trigger 1 and add entry to map with trigger and corresponding list of expected Events
                // for response verification
                {
                    // create trigger and add to request trigger list
                    PvConditionTrigger trigger = PvConditionTrigger.newBuilder()
                            .setPvName("S02-GCC01")
                            .setCondition(PvConditionTrigger.PvCondition.PV_CONDITION_LESS_EQ)
                            .setValue(DataValue.newBuilder().setDoubleValue(0.2).build())
                            .build();
                    requestTriggers.add(trigger);

                    // create list of expected Events for trigger
                    final List<SubscribeDataEventResponse.Event> triggerExpectedEvents = new ArrayList<>();

                    // create TriggeredEvent and add to list, data value 0.0
                    {
                        final SubscribeDataEventResponse.Event event = SubscribeDataEventResponse.Event.newBuilder()
                                .setTrigger(trigger)
                                .setDataValue(DataValue.newBuilder().setDoubleValue(0).build())
                                .setEventTime(Timestamp.newBuilder().setEpochSeconds(startSeconds).build())
                                .build();
                        triggerExpectedEvents.add(event);
                    }

                    // create TriggeredEvent and add to list, data value 0.1
                    {
                        final SubscribeDataEventResponse.Event event = SubscribeDataEventResponse.Event.newBuilder()
                                .setTrigger(trigger)
                                .setDataValue(DataValue.newBuilder().setDoubleValue(0.1).build())
                                .setEventTime(Timestamp.newBuilder().setEpochSeconds(startSeconds).setNanoseconds(100000000).build())
                                .build();
                        triggerExpectedEvents.add(event);
                    }

                    // create TriggeredEvent and add to list, data value 0.2
                    {
                        final SubscribeDataEventResponse.Event event = SubscribeDataEventResponse.Event.newBuilder()
                                .setTrigger(trigger)
                                .setDataValue(DataValue.newBuilder().setDoubleValue(0.2).build())
                                .setEventTime(Timestamp.newBuilder().setEpochSeconds(startSeconds).setNanoseconds(200000000).build())
                                .build();
                        triggerExpectedEvents.add(event);
                    }

                    // add entry to response validation map with trigger and list of expected events
                    expectedEventResponses3.put(trigger, triggerExpectedEvents);
                    expectedResponseCount3 += triggerExpectedEvents.size();
                }

                // create trigger 2 and add entry to map with trigger and corresponding list of expected Events
                // for response verification
                {
                    // create trigger and add to request trigger list
                    PvConditionTrigger trigger = PvConditionTrigger.newBuilder()
                            .setPvName("S02-GCC02")
                            .setCondition(PvConditionTrigger.PvCondition.PV_CONDITION_GREATER_EQ)
                            .setValue(DataValue.newBuilder().setDoubleValue(9.8).build())
                            .build();
                    requestTriggers.add(trigger);

                    // create list of expected Events for trigger
                    final List<SubscribeDataEventResponse.Event> triggerExpectedEvents = new ArrayList<>();

                    // create TriggeredEvent and add to list, data value 9.8
                    {
                        final SubscribeDataEventResponse.Event event = SubscribeDataEventResponse.Event.newBuilder()
                                .setTrigger(trigger)
                                .setDataValue(DataValue.newBuilder().setDoubleValue(9.8).build())
                                .setEventTime(Timestamp.newBuilder().setEpochSeconds(startSeconds+9).setNanoseconds(800000000).build())
                                .build();
                        triggerExpectedEvents.add(event);
                    }

                    // create TriggeredEvent and add to list, data value 9.9
                    {
                        final SubscribeDataEventResponse.Event event = SubscribeDataEventResponse.Event.newBuilder()
                                .setTrigger(trigger)
                                .setDataValue(DataValue.newBuilder().setDoubleValue(9.9).build())
                                .setEventTime(Timestamp.newBuilder().setEpochSeconds(startSeconds+9).setNanoseconds(900000000).build())
                                .build();
                        triggerExpectedEvents.add(event);
                    }

                    // add entry to response validation map with trigger and list of expected events
                    expectedEventResponses3.put(trigger, triggerExpectedEvents);
                    expectedResponseCount3 += triggerExpectedEvents.size();
                }

                // create params object (including trigger params list) for building protobuf request from params
                requestParams3 =
                        new IngestionStreamTestBase.SubscribeDataEventRequestParams(requestTriggers, null, null, null);

                // call subscribeDataEvent() to initiate subscription before running ingestion
                final boolean expectReject = false;
                final String expectedRejectMessage = "";
                subscribeDataEventCall3 =
                        ingestionStreamServiceWrapper.initiateSubscribeDataEventRequest(
                                requestParams3,
                                expectedResponseCount3,
                                0, expectReject,
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

            // request 1: verify subscribeDataEvent() responses and close request stream
            ingestionStreamServiceWrapper.verifySubscribeDataEventResponse(
                    (IngestionStreamTestBase.SubscribeDataEventResponseObserver) subscribeDataEventCall.responseObserver(),
                    expectedEventResponses1, null, 0, null);
            subscribeDataEventCall.requestObserver().onCompleted();

            // request 2: verify subscribeDataEvent() responses and close request stream
            ingestionStreamServiceWrapper.verifySubscribeDataEventResponse(
                    (IngestionStreamTestBase.SubscribeDataEventResponseObserver) subscribeDataEventCall2.responseObserver(),
                    expectedEventResponses2, null, 0, null);
            subscribeDataEventCall2.requestObserver().onCompleted();

            // request 3: verify subscribeDataEvent() responses and close request stream
            ingestionStreamServiceWrapper.verifySubscribeDataEventResponse(
                    (IngestionStreamTestBase.SubscribeDataEventResponseObserver) subscribeDataEventCall3.responseObserver(),
                    expectedEventResponses3, null, 0, null);
            subscribeDataEventCall3.requestObserver().onCompleted();
        }
    }

    /**
     * This test case provides negative test coverage for a subscribeDataEvent() reject due to data type mismatch.
     */
    @Test
    public void testSubscribeDataEventTriggerRejectDataTypeMismatch() {

        final long startSeconds = Instant.now().getEpochSecond();

        {
            // Pre-populate some data in the archive for the PVs that we will be using.
            // This is necessary because validation is performed that data exists in the archive for the
            // PV names in subscribeData() requests.
            ingestionServiceWrapper.simpleIngestionScenario(
                    startSeconds-600,
                    true);
        }

        {
            // Negative test case: Data type for PvConditionTrigger in request mismatches actual data.
            // Request uses integer, actual data is double.
            IngestionStreamTestBase.SubscribeDataEventCall subscribeDataEventCall;
            IngestionStreamTestBase.SubscribeDataEventRequestParams requestParams1;
            Map<PvConditionTrigger, List<SubscribeDataEventResponse.Event>> expectedEventResponses1 = new HashMap<>();
            int expectedResponseCount1 = 0;
            {
                // create list of triggers for request
                List<PvConditionTrigger> requestTriggers = new ArrayList<>();

                // create trigger, add entry to response verification map with trigger and expected Events
                {
                    PvConditionTrigger trigger = PvConditionTrigger.newBuilder()
                            .setPvName("S01-BPM01")
                            .setCondition(PvConditionTrigger.PvCondition.PV_CONDITION_EQUAL_TO)
                            .setValue(DataValue.newBuilder().setIntValue(5).build())  // INVALID DATA TYPE FOR PV
                            .build();
                    requestTriggers.add(trigger);
                    final List<SubscribeDataEventResponse.Event> triggerExpectedEvents = new ArrayList<>();
                    final DataValue eventDataValue = DataValue.newBuilder().setDoubleValue(5.0).build();
                    final SubscribeDataEventResponse.Event event = SubscribeDataEventResponse.Event.newBuilder()
                            .setTrigger(trigger)
                            .setDataValue(eventDataValue)
                            .setEventTime(Timestamp.newBuilder().setEpochSeconds(startSeconds+5).build())
                            .build();
                    triggerExpectedEvents.add(event);
                    expectedEventResponses1.put(trigger, triggerExpectedEvents);
                    expectedResponseCount1 += triggerExpectedEvents.size();
                }

                // create params object (including trigger params list) for building protobuf request from params
                requestParams1 =
                        new IngestionStreamTestBase.SubscribeDataEventRequestParams(requestTriggers, null, null, null);

                // call subscribeDataEvent() to initiate subscription before running ingestion
                final boolean expectReject = false;
                final String expectedRejectMessage = "";
                subscribeDataEventCall =
                        ingestionStreamServiceWrapper.initiateSubscribeDataEventRequest(
                                requestParams1,
                                expectedResponseCount1,
                                0,
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
                ingestionScenarioResult =
                        ingestionServiceWrapper.simpleIngestionScenario(startSeconds, false);
            }

            // request 1: verify error for data type mismatch between specified PvConditionTrigger and actual data.
            final String expectedErrorMessage =
                    "PvConditionTrigger type mismatch PV name: S01-BPM01 PV data type: DOUBLEVALUE trigger value data type: INTVALUE";
            ingestionStreamServiceWrapper.verifySubscribeDataEventError(
                    (IngestionStreamTestBase.SubscribeDataEventResponseObserver) subscribeDataEventCall.responseObserver(),
                    expectedErrorMessage);
            subscribeDataEventCall.requestObserver().onCompleted();
        }
    }

    /**
     * This test case provides negative test coverage for a subscribeDataEvent() reject due to invalid PV name.
     */
    @Test
    public void testSubscribeDataEventRejectInvalidPvName() {

        final long startSeconds = Instant.now().getEpochSecond();

        {
            // Pre-populate some data in the archive for the PVs that we will be using.
            // This is necessary because validation is performed that data exists in the archive for the
            // PV names in subscribeData() requests.
            ingestionServiceWrapper.simpleIngestionScenario(
                    startSeconds - 600,
                    true);
        }

        {
            // negative test case, request is rejected because it uses invalid PV name
            IngestionStreamTestBase.SubscribeDataEventCall subscribeDataEventCall;
            IngestionStreamTestBase.SubscribeDataEventRequestParams requestParams1;
            Map<PvConditionTrigger, List<SubscribeDataEventResponse.Event>> expectedEventResponses1 = new HashMap<>();
            int expectedResponseCount1 = 0;
            {
                // create list of triggers for request
                List<PvConditionTrigger> requestTriggers = new ArrayList<>();

                // create trigger, add entry to response verification map with trigger and expected Events
                {
                    PvConditionTrigger trigger = PvConditionTrigger.newBuilder()
                            .setPvName("junk")
                            .setCondition(PvConditionTrigger.PvCondition.PV_CONDITION_EQUAL_TO)
                            .setValue(DataValue.newBuilder().setDoubleValue(5.0).build())
                            .build();
                    requestTriggers.add(trigger);
                    final List<SubscribeDataEventResponse.Event> triggerExpectedEvents = new ArrayList<>();
                    final DataValue eventDataValue = DataValue.newBuilder().setDoubleValue(5.0).build();
                    final SubscribeDataEventResponse.Event event = SubscribeDataEventResponse.Event.newBuilder()
                            .setTrigger(trigger)
                            .setDataValue(eventDataValue)
                            .setEventTime(Timestamp.newBuilder().setEpochSeconds(startSeconds + 5).build())
                            .build();
                    triggerExpectedEvents.add(event);
                    expectedEventResponses1.put(trigger, triggerExpectedEvents);
                    expectedResponseCount1 += triggerExpectedEvents.size();
                }

                // create params object (including trigger params list) for building protobuf request from params
                requestParams1 =
                        new IngestionStreamTestBase.SubscribeDataEventRequestParams(requestTriggers, null, null, null);

                // call subscribeDataEvent() to initiate subscription before running ingestion
                final boolean expectReject = true;
                final String expectedRejectMessage = "PV names not found in archive: [junk]";
                subscribeDataEventCall =
                        ingestionStreamServiceWrapper.initiateSubscribeDataEventRequest(
                                requestParams1,
                                expectedResponseCount1,
                                0,
                                expectReject,
                                expectedRejectMessage);
            }
        }
    }

    /**
     * This test case provides negative test coverage for a subscribeDataEvent() rejects for different scenarios related
     * to invalid trigger specifications.
     */
    @Test
    public void testSubscribeDataEventTriggerReject() {

        final long startSeconds = Instant.now().getEpochSecond();

        // reject reason: empty list of triggers
        {
            IngestionStreamTestBase.SubscribeDataEventCall subscribeDataEventCall;
            IngestionStreamTestBase.SubscribeDataEventRequestParams requestParams;

            // create list of triggers for request
            List<PvConditionTrigger> emptyTriggersList = new ArrayList<>(); // EMPTY TRIGGER LIST CAUSES REJECT

            // create params object (including trigger params list) for building protobuf request from params
            requestParams =
                    new IngestionStreamTestBase.SubscribeDataEventRequestParams(emptyTriggersList, null, null, null);

            // call subscribeDataEvent() to initiate subscription before running ingestion
            final int expectedResponseCount = 0;
            final boolean expectReject = true; // EXPECT REJECT
            final String expectedRejectMessage = "SubscribeDataEventRequest.triggers must be specified"; // EXPECTED ERROR MESSAGE
            subscribeDataEventCall =
                    ingestionStreamServiceWrapper.initiateSubscribeDataEventRequest(
                            requestParams,
                            expectedResponseCount,
                            0, expectReject,
                            expectedRejectMessage);
        }

        // reject reason: blank trigger PV name
        {
            IngestionStreamTestBase.SubscribeDataEventCall subscribeDataEventCall;
            IngestionStreamTestBase.SubscribeDataEventRequestParams requestParams;

            // create list of triggers for request
            List<PvConditionTrigger> requestTriggers = new ArrayList<>();

            // create trigger with blank PV name
            {
                PvConditionTrigger trigger = PvConditionTrigger.newBuilder()
                        .setPvName("") // BLANK PV NAME
                        .setCondition(PvConditionTrigger.PvCondition.PV_CONDITION_EQUAL_TO)
                        .setValue(DataValue.newBuilder().setDoubleValue(5.0).build())
                        .build();
                requestTriggers.add(trigger);
                final List<SubscribeDataEventResponse.Event> triggerExpectedEvents = new ArrayList<>();
            }

            // create params object (including trigger params list) for building protobuf request from params
            requestParams =
                    new IngestionStreamTestBase.SubscribeDataEventRequestParams(requestTriggers, null, null, null);

            // call subscribeDataEvent() to initiate subscription before running ingestion
            final int expectedResponseCount = 0;
            final boolean expectReject = true;  // EXPECT REJECT
            final String expectedRejectMessage = "SubscribeDataEventRequest PvConditionTrigger.pvName must be specified";
            subscribeDataEventCall =
                    ingestionStreamServiceWrapper.initiateSubscribeDataEventRequest(
                            requestParams,
                            expectedResponseCount,
                            0, expectReject,
                            expectedRejectMessage);
        }

        // reject reason: trigger condition not specified
        {
            IngestionStreamTestBase.SubscribeDataEventCall subscribeDataEventCall;
            IngestionStreamTestBase.SubscribeDataEventRequestParams requestParams;

            // create list of triggers for request
            List<PvConditionTrigger> requestTriggers = new ArrayList<>();

            // create trigger with blank PV name
            {
                PvConditionTrigger trigger = PvConditionTrigger.newBuilder()
                        .setPvName("S01-BPM01")
                        // DON'T SET CONDITION:  .setCondition(PvConditionTrigger.PvCondition.PV_CONDITION_EQUAL_TO)
                        .setValue(DataValue.newBuilder().setDoubleValue(5.0).build())
                        .build();
                requestTriggers.add(trigger);
                final List<SubscribeDataEventResponse.Event> triggerExpectedEvents = new ArrayList<>();
            }

            // create params object (including trigger params list) for building protobuf request from params
            requestParams =
                    new IngestionStreamTestBase.SubscribeDataEventRequestParams(requestTriggers, null, null, null);

            // call subscribeDataEvent() to initiate subscription before running ingestion
            final int expectedResponseCount = 0;
            final boolean expectReject = true; // EXPECT REJECT
            final String expectedRejectMessage = "SubscribeDataEventRequest PvConditionTrigger.condition must be specified";
            subscribeDataEventCall =
                    ingestionStreamServiceWrapper.initiateSubscribeDataEventRequest(
                            requestParams,
                            expectedResponseCount,
                            0, expectReject,
                            expectedRejectMessage);
        }

    }

}
