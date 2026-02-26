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
 * This integration test covers the use of protobuf ImageColumns in the MLDP APIs.
 * Uses a dual PV approach where scalar columns serve as trigger PVs and image columns serve as target PVs
 * for data event subscriptions, since binary columns cannot function as trigger PVs.
 */
public class ImageColumnIT extends GrpcIntegrationTestBase {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    /**
     * This test case provides full MLDP API coverage for use of ImageColumns.
     * Registers a provider, which is required before using the ingestion APIs.  Uses the data ingestion API
     * to send an IngestDataRequest whose IngestionDataFrame contains a ImageColumn data structure.
     * Uses the time-series data query API to retrieve the bucket containing the ImageColumn sent in
     * the ingestion request.  Confirms that the ImageColumn retrieved via the query API matches the
     * column sent in the ingestion request, using ImageColumn.equals() which compares column name,
     * imageDescriptor, and data values in the two columns.  Subscribes for PV data via the subscribeData() API
     * method and confirms that the data received in the subscription response stream matches the ingested data.
     * Registers via subscribeDataEvent() for data events using a dual PV approach where scalar columns serve
     * as triggers and image columns serve as targets, and confirms that the appropriate responses are received.
     */
    @Test
    public void testImageColumnIntegration() throws Exception {

        final String triggerPvName = "testTrigger-" + Instant.now().toEpochMilli();  // scalar PV for event triggers
        final String imagePvName = "testImage-" + Instant.now().toEpochMilli();     // image PV for event targets
        final List<String> allColumnNames = Arrays.asList(triggerPvName, imagePvName);
        final List<String> triggerColumnNames = Arrays.asList(triggerPvName);
        final List<String> imageColumnNames = Arrays.asList(imagePvName);
        
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
        ImageColumn requestImageColumn;

        // ingest initial data with both scalar and image columns using dual PV approach
        {
            // specify explicit DoubleColumn data for trigger PV (scalar)
            final List<DoubleColumn> doubleColumns = new ArrayList<>();
            DoubleColumn.Builder doubleColumnBuilder = DoubleColumn.newBuilder();
            doubleColumnBuilder.setName(triggerPvName);
            doubleColumnBuilder.addValues(10.0);
            doubleColumnBuilder.addValues(20.0);
            requestTriggerColumn = doubleColumnBuilder.build();
            doubleColumns.add(requestTriggerColumn);

            // specify explicit ImageColumn data for target PV
            final List<ImageColumn> imageColumns = new ArrayList<>();
            ImageColumn.Builder imageColumnBuilder = ImageColumn.newBuilder();
            imageColumnBuilder.setName(imagePvName);
            
            // Set ImageDescriptor
            ImageDescriptor imageDescriptor = ImageDescriptor.newBuilder()
                    .setWidth(640)
                    .setHeight(480)
                    .setChannels(3) // RGB
                    .setEncoding("raw")
                    .build();
            imageColumnBuilder.setImageDescriptor(imageDescriptor);
            
            // Add image values for 2 samples (each as serialized byte array)
            // Sample 1: Mock image data (640x480x3 = 921600 bytes would be too large, use smaller mock data)
            byte[] image1Data = new byte[100]; // Mock image data
            for (int i = 0; i < image1Data.length; i++) {
                image1Data[i] = (byte) (i % 256); // Fill with test pattern
            }
            imageColumnBuilder.addImages(com.google.protobuf.ByteString.copyFrom(image1Data));
            
            // Sample 2: Mock image data with different pattern
            byte[] image2Data = new byte[120]; // Different size to test variable-length handling
            for (int i = 0; i < image2Data.length; i++) {
                image2Data[i] = (byte) ((i * 2) % 256); // Different test pattern
            }
            imageColumnBuilder.addImages(com.google.protobuf.ByteString.copyFrom(image2Data));
            
            requestImageColumn = imageColumnBuilder.build();
            imageColumns.add(requestImageColumn);

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
            initialIngestionRequestParams.setImageColumnList(imageColumns); // add image target column

            final IngestDataRequest request =
                    IngestionTestBase.buildIngestionRequest(initialIngestionRequestParams);

            ingestionServiceWrapper.sendAndVerifyIngestData(initialIngestionRequestParams, request);
        }

        // positive queryData() test case for image column
        {
            // select 1 second of data for image pv
            final long beginSeconds = firstSeconds;
            final long beginNanos = firstNanos;
            final long endSeconds = beginSeconds + 1L;
            final long endNanos = 0L;

            final int numBucketsExpected = 1;
            final boolean expectReject = false;
            final String expectedRejectMessage = "";

            final QueryTestBase.QueryDataRequestParams params =
                    new QueryTestBase.QueryDataRequestParams(
                            imageColumnNames,
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
                assertTrue("Query result should contain ImageColumn", queryResultBucket.getDataValues().hasImageColumn());
                assertEquals(requestImageColumn, queryResultBucket.getDataValues().getImageColumn());
            }
        }

        // create a data subscription for image PV, verification succeeds because data have been ingested
        SubscribeDataUtility.SubscribeDataCall subscribeDataCall;
        {
            final int expectedResponseCount = 1;
            final boolean expectReject = false;
            final String expectedRejectMessage = "";
            subscribeDataCall =
                    ingestionServiceWrapper.initiateSubscribeDataRequest(
                            imageColumnNames, expectedResponseCount, expectReject, expectedRejectMessage);
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

            // DataEventOperation details for params - image PV as target
            final List<String> targetPvs = List.of(imagePvName); // Image PV as target
            final long offset = -3_000_000_000L; // 3 seconds negative trigger time offset
            final long duration = 5_000_000_000L; // 5 second duration

            // add entry for event to response verification map with details about expected EventData responses
            final int expectedDataBucketCount = 1;
            final List<Instant> instantList = List.of(Instant.ofEpochSecond(firstSeconds + 1));
            final Map<String, List<Instant>> pvInstantMap = new HashMap<>();
            expectedEventDataResponses.put(event, pvInstantMap);
            pvInstantMap.put(imagePvName, instantList); // Expect image PV data in response

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
        ImageColumn subscriptionImageColumn;
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

            // specify explicit ImageColumn data for target PV
            final List<ImageColumn> imageColumns = new ArrayList<>();
            ImageColumn.Builder imageColumnBuilder = ImageColumn.newBuilder();
            imageColumnBuilder.setName(imagePvName);
            
            // Set same ImageDescriptor as initial data
            ImageDescriptor imageDescriptor = ImageDescriptor.newBuilder()
                    .setWidth(640)
                    .setHeight(480)
                    .setChannels(3) // RGB
                    .setEncoding("raw")
                    .build();
            imageColumnBuilder.setImageDescriptor(imageDescriptor);
            
            // Add new image values for 2 samples
            // Sample 1: Updated image data
            byte[] newImage1Data = new byte[80]; // Different size for updated image
            for (int i = 0; i < newImage1Data.length; i++) {
                newImage1Data[i] = (byte) ((i * 3) % 256); // New test pattern
            }
            imageColumnBuilder.addImages(com.google.protobuf.ByteString.copyFrom(newImage1Data));
            
            // Sample 2: Updated image data
            byte[] newImage2Data = new byte[90]; // Different size
            for (int i = 0; i < newImage2Data.length; i++) {
                newImage2Data[i] = (byte) ((i * 4) % 256); // New test pattern
            }
            imageColumnBuilder.addImages(com.google.protobuf.ByteString.copyFrom(newImage2Data));
            
            subscriptionImageColumn = imageColumnBuilder.build();
            imageColumns.add(subscriptionImageColumn);

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
            subscriptionRequestParams.setImageColumnList(imageColumns); // add image target column

            final IngestDataRequest request =
                    IngestionTestBase.buildIngestionRequest(subscriptionRequestParams);

            ingestionServiceWrapper.sendAndVerifyIngestData(subscriptionRequestParams, request);
        }

        // check that expected subscribeData() response is received for image PV
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

            // verify response contains expected ImageColumn data
            assertTrue(subscriptionResponse.hasSubscribeDataResult());
            assertEquals(1, subscriptionResponse.getSubscribeDataResult().getDataBucketsCount());
            final DataBucket receivedBucket = subscriptionResponse.getSubscribeDataResult().getDataBuckets(0);
            assertTrue("Subscription response should contain ImageColumn", receivedBucket.getDataValues().hasImageColumn());
            // Note: subscription receives the latest data, which is from the second ingestion (subscriptionImageColumn)
            final ImageColumn receivedColumn = receivedBucket.getDataValues().getImageColumn();
            assertEquals(imagePvName, receivedColumn.getName());
            assertEquals(640, receivedColumn.getImageDescriptor().getWidth());
            assertEquals(480, receivedColumn.getImageDescriptor().getHeight());
            assertEquals(3, receivedColumn.getImageDescriptor().getChannels());
            assertEquals("raw", receivedColumn.getImageDescriptor().getEncoding());
            assertTrue("Should have received image data", receivedColumn.getImagesCount() > 0);
        }

        // check that expected subscribeDataEvent() responses are received for image PV
        final List<DataBucket> responseDataBuckets = ingestionStreamServiceWrapper.verifySubscribeDataEventResponse(
                (IngestionStreamTestBase.SubscribeDataEventResponseObserver) subscribeDataEventCall.responseObserver(),
                expectedEventResponses,
                expectedEventDataResponses,
                DataValues.ValuesCase.IMAGECOLUMN
        );

        // verify the data event response contains the expected ImageColumn data
        assertEquals(1, responseDataBuckets.size());
        final DataBucket eventDataBucket = responseDataBuckets.get(0);
        assertTrue("Event response should contain ImageColumn", eventDataBucket.getDataValues().hasImageColumn());
        assertEquals(subscriptionImageColumn, responseDataBuckets.get(0).getDataValues().getImageColumn());
        ingestionStreamServiceWrapper.closeSubscribeDataEventCall(subscribeDataEventCall);
    }
}