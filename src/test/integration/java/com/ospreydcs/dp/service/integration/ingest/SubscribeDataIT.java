package com.ospreydcs.dp.service.integration.ingest;

import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataRequest;
import com.ospreydcs.dp.service.ingest.IngestionTestBase;
import com.ospreydcs.dp.service.ingest.utility.SubscribeDataUtility;
import com.ospreydcs.dp.service.integration.GrpcIntegrationTestBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.*;

import java.time.Instant;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class SubscribeDataIT extends GrpcIntegrationTestBase {

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

    @Test
    public void testRejectEmptyPvNamesList() {
        {
            // negative test case for subscribeData() with empty list of PV names

            final List<String> subscriptionPvNames = List.of();
            final int expectedResponseCount = 30;
            final boolean expectReject = true;
            final String expectedRejectMessage = "SubscribeDataRequest.NewSubscription.pvNames list must not be empty";
            final SubscribeDataUtility.SubscribeDataCall subscribeDataCall =
                    ingestionServiceWrapper.initiateSubscribeDataRequest(
                            subscriptionPvNames, expectedResponseCount, expectReject, expectedRejectMessage);
        }
    }

    @Test
    public void testRejectNoRequestPayload() {
        {
            // negative test case for subscribeData() with no request payload (NewSubscription or CancelSubscription)

            final List<String> subscriptionPvNames = List.of("S01-GCC01", "S02-GCC01", "S03-BPM01");
            final int expectedResponseCount = 30;
            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "received unknown request, expected NewSubscription or CancelSubscription";
            final SubscribeDataRequest request = buildSubscribeDataRequestNoPayload();
            final SubscribeDataUtility.SubscribeDataCall subscribeDataCall =
                    ingestionServiceWrapper.initiateSubscribeDataRequest(
                            request, expectedResponseCount, expectReject, expectedRejectMessage);
        }
    }

    @Test
    public void testRejectMultipleNewSubscriptionMessages() {

        // negative test case for subscribeData(), sending multiple NewSubscription messages causes reject

        final long startSeconds = Instant.now().getEpochSecond();

        {
            // Pre-populate some data in the archive for the PVs that we will be using.
            // This is necessary because validation is performed that data exists in the archive for the
            // PV names in subscribeData() requests.
            ingestionServiceWrapper.simpleIngestionScenario(
                    startSeconds-600,
                    true);
        }

        final List<String> subscriptionPvNames = List.of("S09-GCC01", "S09-GCC01", "S09-BPM01");
        final int expectedResponseCount = 30;
        final boolean expectReject = false;
        final String expectedRejectMessage = "";
        final SubscribeDataUtility.SubscribeDataCall subscribeDataCall =
                ingestionServiceWrapper.initiateSubscribeDataRequest(
                        subscriptionPvNames, expectedResponseCount, expectReject, expectedRejectMessage);
        final SubscribeDataRequest duplicateRequest =
                SubscribeDataUtility.buildSubscribeDataRequest(subscriptionPvNames);

        // send duplicate NewSubscription message in request stream
        new Thread(() -> {
            subscribeDataCall.requestObserver().onNext(duplicateRequest);
        }).start();

        final String expectedDuplicateRejectMessage =
                "multiple NewSubscription messages not supported in request stream";
        final IngestionTestBase.SubscribeDataResponseObserver responseObserver =
                (IngestionTestBase.SubscribeDataResponseObserver) subscribeDataCall.responseObserver();
        responseObserver.awaitCloseLatch();
        assertTrue(responseObserver.isError());
        assertTrue(responseObserver.getErrorMessage().contains(expectedDuplicateRejectMessage));
    }

    @Test
    public void testRejectInvalidPvName() {

        // negative test case for subscribeData(), sending request containing an invalid PV name

        final long startSeconds = Instant.now().getEpochSecond();

        {
            // Pre-populate some data in the archive for the PVs that we will be using.
            // This is necessary because validation is performed that data exists in the archive for the
            // PV names in subscribeData() requests.
            ingestionServiceWrapper.simpleIngestionScenario(
                    startSeconds-600,
                    true);
        }

        final List<String> subscriptionPvNames = List.of("junk");
        final int expectedResponseCount = 30;
        final boolean expectReject = true;
        String expectedRejectMessage = "PV names not found in archive: [junk]";
        final SubscribeDataUtility.SubscribeDataCall subscribeDataCall =
                ingestionServiceWrapper.initiateSubscribeDataRequest(
                        subscriptionPvNames, expectedResponseCount, expectReject, expectedRejectMessage);
    }

    @Test
    public void testSubscribeDataPositive() {

        {
            final long startSeconds = Instant.now().getEpochSecond();

            {
                // Pre-populate some data in the archive for the PVs that we will be using.
                // This is necessary because validation is performed that data exists in the archive for the
                // PV names in subscribeData() requests.
                ingestionServiceWrapper.simpleIngestionScenario(
                        startSeconds-600,
                        true);
            }

            // positive test cases for subscribeData() with 3 different alternatives for canceling subscription:
            // 1) implicit close on ingestion server shutdown
            // 2) explicit CancelSubscription message in request stream and
            // 3) client closing request stream without canceling.

            SubscribeDataUtility.SubscribeDataCall subscribeDataCall1;
            List<String> subscriptionPvNames1;
            SubscribeDataUtility.SubscribeDataCall subscribeDataCall2;
            List<String> subscriptionPvNames2;
            SubscribeDataUtility.SubscribeDataCall subscribeDataCall3;
            List<String> subscriptionPvNames3;

            {
                // 1) invoke subscribeData with call that will be closed implicitly by server
                subscriptionPvNames1 = List.of("S01-GCC01", "S02-GCC01", "S03-BPM01");
                final int expectedResponseCount = 30;
                final boolean expectReject = false;
                final String expectedRejectMessage = "";
                subscribeDataCall1 =
                        ingestionServiceWrapper.initiateSubscribeDataRequest(
                                subscriptionPvNames1, expectedResponseCount, expectReject, expectedRejectMessage);
            }

            {
                // 2) invoke subscribeData with call that will be closed explicitly by client with cancel message
                subscriptionPvNames2 = List.of("S01-GCC02", "S02-GCC02", "S03-BPM02");
                final int expectedResponseCount = 30;
                final boolean expectReject = false;
                final String expectedRejectMessage = "";
                subscribeDataCall2 =
                        ingestionServiceWrapper.initiateSubscribeDataRequest(
                                subscriptionPvNames2, expectedResponseCount, expectReject, expectedRejectMessage);
            }

            {
                // 3) invoke subscribeData with call that will be closed explicitly by client with cancel message
                subscriptionPvNames3 = List.of("S01-GCC03", "S02-GCC03", "S03-BPM03");
                final int expectedResponseCount = 30;
                final boolean expectReject = false;
                final String expectedRejectMessage = "";
                subscribeDataCall3 =
                        ingestionServiceWrapper.initiateSubscribeDataRequest(
                                subscriptionPvNames3, expectedResponseCount, expectReject, expectedRejectMessage);
            }

            // run a simple ingestion scenario that will publish to all 3 subscriptions
            GrpcIntegrationIngestionServiceWrapper.IngestionScenarioResult ingestionScenarioResult;
            {
                // create some data for testing query APIs
                // create data for 10 sectors, each containing 3 gauges and 3 bpms
                // named with prefix "S%02d-" followed by "GCC%02d" or "BPM%02d"
                // with 10 measurements per bucket, 1 bucket per second, and 10 buckets per pv
                ingestionScenarioResult = ingestionServiceWrapper.simpleIngestionScenario(startSeconds, false);
            }

            // verify all 3 subscriptions received expected messages
            ingestionServiceWrapper.verifySubscribeDataResponse(
                    (IngestionTestBase.SubscribeDataResponseObserver) subscribeDataCall1.responseObserver(),
                    subscriptionPvNames1,
                    ingestionScenarioResult.validationMap()
            );
            ingestionServiceWrapper.verifySubscribeDataResponse(
                    (IngestionTestBase.SubscribeDataResponseObserver) subscribeDataCall2.responseObserver(),
                    subscriptionPvNames2,
                    ingestionScenarioResult.validationMap()
            );
            ingestionServiceWrapper.verifySubscribeDataResponse(
                    (IngestionTestBase.SubscribeDataResponseObserver) subscribeDataCall3.responseObserver(),
                    subscriptionPvNames3,
                    ingestionScenarioResult.validationMap()
            );

            // 2) cancel subscription with explicit cancel message
            ingestionServiceWrapper.cancelSubscribeDataCall(subscribeDataCall2);

            // 3) cancel subscription by closing client request stream
            ingestionServiceWrapper.closeSubscribeDataCall(subscribeDataCall3);
        }
    }

    public static SubscribeDataRequest buildSubscribeDataRequestNoPayload() {
        return SubscribeDataRequest.newBuilder().build();
    }

}
