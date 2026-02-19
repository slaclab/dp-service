package com.ospreydcs.dp.service.ingestionstream;

import com.ospreydcs.dp.grpc.v1.ingestionstream.DataEventOperation;
import com.ospreydcs.dp.grpc.v1.ingestionstream.PvConditionTrigger;
import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventRequest;
import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.fail;

/**
 * Base class for unit and integration tests covering the Ingestion Stream Service APIs.  Provides utilities for those tests,
 * including 1) params objects for creating protobuf API requests, 2) methods for building protobuf API requests from
 * the params, 3) observers for the API response streams, and 4) utilities for verifying the API results.
 */
public class IngestionStreamTestBase {

    public static final class SubscribeDataEventRequestParams {
        private final List<PvConditionTrigger> triggers;
        private final List<String> targetPvs;
        private final Long offset;
        private final Long duration;
        public boolean noWindow = false;

        public SubscribeDataEventRequestParams(
                List<PvConditionTrigger> triggers,
                List<String> targetPvs,
                Long offset,
                Long duration
        ) {
            this.triggers = triggers;
            this.targetPvs = targetPvs;
            this.offset = offset;
            this.duration = duration;
        }

        public List<PvConditionTrigger> triggers() {
            return triggers;
        }

        public List<String> targetPvs() {
            return targetPvs;
        }

        public Long offset() {
            return offset;
        }

        public Long duration() {
            return duration;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;
            var that = (SubscribeDataEventRequestParams) obj;
            return Objects.equals(this.triggers, that.triggers) &&
                    Objects.equals(this.targetPvs, that.targetPvs) &&
                    Objects.equals(this.offset, that.offset) &&
                    Objects.equals(this.duration, that.duration);
        }

        @Override
        public int hashCode() {
            return Objects.hash(triggers, targetPvs, offset, duration);
        }

        @Override
        public String toString() {
            return "SubscribeDataEventRequestParams[" +
                    "triggers=" + triggers + ", " +
                    "targetPvs=" + targetPvs + ", " +
                    "offset=" + offset + ", " +
                    "duration=" + duration + ']';
        }

        }

    public record SubscribeDataEventCall(
            StreamObserver<SubscribeDataEventRequest> requestObserver,
            StreamObserver<SubscribeDataEventResponse> responseObserver
    ) {
    }
    
    public static class SubscribeDataEventResponseObserver implements StreamObserver<SubscribeDataEventResponse> {

        // instance variables
        CountDownLatch ackLatch = null;
        CountDownLatch responseLatch = null;
        CountDownLatch closeLatch = null;
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        private final List<SubscribeDataEventResponse> responseList = Collections.synchronizedList(new ArrayList<>());
        private final AtomicBoolean isError = new AtomicBoolean(false);

        public SubscribeDataEventResponseObserver(int expectedResponseCount) {
            this.ackLatch = new CountDownLatch(1);
            this.responseLatch = new CountDownLatch(expectedResponseCount);
            this.closeLatch = new CountDownLatch(1);
        }

        public void awaitAckLatch() {
            try {
                final boolean await = ackLatch.await(1, TimeUnit.MINUTES);
                if ( ! await) {
                    fail("timed out waiting for ack latch");
                }
            } catch (InterruptedException e) {
                final String errorMsg = "InterruptedException waiting for ackLatch";
                System.err.println(errorMsg);
                isError.set(true);
                errorMessageList.add(errorMsg);
            }
        }

        public void awaitResponseLatch() {
            try {
                final boolean await = responseLatch.await(10, TimeUnit.SECONDS);
                if ( ! await) {
                    fail("timed out waiting for response latch count: " + responseLatch.getCount());
                }
            } catch (InterruptedException e) {
                final String errorMsg = "InterruptedException waiting for responseLatch";
                System.err.println(errorMsg);
                isError.set(true);
                errorMessageList.add(errorMsg);
            }
        }
        
        public void awaitCloseLatch() {
            try {
                closeLatch.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                final String errorMsg = "InterruptedException waiting for closeLatch";
                System.err.println(errorMsg);
                isError.set(true);
                errorMessageList.add(errorMsg);
            }
        }



        public List<SubscribeDataEventResponse> getResponseList() {
            return responseList;
        }

        public boolean isError() {
            return isError.get();
        }

        public String getErrorMessage() {
            if (!errorMessageList.isEmpty()) {
                return errorMessageList.get(0);
            } else {
                return "";
            }
        }

        @Override
        public void onNext(SubscribeDataEventResponse response) {

            switch (response.getResultCase()) {

                case EXCEPTIONALRESULT -> {
                    final String errorMsg = response.getExceptionalResult().getMessage();
                    System.err.println("SubscribeDataEventResponseOberver received ExceptionalResult: " + errorMsg);
                    isError.set(true);
                    errorMessageList.add(errorMsg);
                    ackLatch.countDown();
                }

                case ACK -> {
                    // decrement ackLatch for ack response
                    System.err.println("SubscribeDataEventResponseOberver received ack");
                    ackLatch.countDown();
                }

                case EVENT -> {
                    // decrement responseLatch for TriggeredEvent response
                    responseList.add(response);
                    responseLatch.countDown();
                }

                case EVENTDATA -> {
                    // decrement responseLatch by number of buckets in EventData response
                    responseList.add(response);
                    final SubscribeDataEventResponse.EventData eventData = response.getEventData();
                    for (int i = 0 ; i < eventData.getDataBucketsCount() ; ++i) {
                        responseLatch.countDown();
                    }
                }

                case RESULT_NOT_SET -> {
                    fail("result case not set");
                }
            }
        }

        @Override
        public void onError(Throwable t) {
            Status status = Status.fromThrowable(t);
            final String errorMsg = "onError status: " + status;
            System.err.println(errorMsg);
            isError.set(true);
            errorMessageList.add(errorMsg);
        }

        @Override
        public void onCompleted() {
            System.out.println("SubscribeDataEventResponseObserver onCompleted");
            closeLatch.countDown();
        }

    }

    public static SubscribeDataEventRequest buildSubscribeDataEventRequest(
            IngestionStreamTestBase.SubscribeDataEventRequestParams requestParams
    ) {
        SubscribeDataEventRequest.NewSubscription.Builder newSubscriptionBuilder =
                SubscribeDataEventRequest.NewSubscription.newBuilder();

        // add triggers to request
        for (PvConditionTrigger trigger : requestParams.triggers) {
            newSubscriptionBuilder.addTriggers(trigger);
        }

        // add DataEventOperation to request
        if (requestParams.targetPvs() != null) {
            DataEventOperation dataEventOperation;

            if (requestParams.noWindow) {
                // causes request to be built without DataEventWindow for reject testing
                dataEventOperation = DataEventOperation.newBuilder()
                        .addAllTargetPvs(requestParams.targetPvs())
                        .build();

            } else {
                // regular request handling
                DataEventOperation.DataEventWindow.TimeInterval timeInterval =
                        DataEventOperation.DataEventWindow.TimeInterval.newBuilder()
                                .setOffset(requestParams.offset)
                                .setDuration(requestParams.duration)
                                .build();
                DataEventOperation.DataEventWindow dataEventWindow =
                        DataEventOperation.DataEventWindow.newBuilder()
                                .setTimeInterval(timeInterval)
                                .build();
                dataEventOperation = DataEventOperation.newBuilder()
                        .addAllTargetPvs(requestParams.targetPvs())
                        .setWindow(dataEventWindow)
                        .build();
            }
            newSubscriptionBuilder.setOperation(dataEventOperation);
        }

        newSubscriptionBuilder.build();

        return SubscribeDataEventRequest.newBuilder().setNewSubscription(newSubscriptionBuilder).build();
    }
    
    public static SubscribeDataEventRequest buildSubscribeDataEventCancelRequest() {

        final SubscribeDataEventRequest.CancelSubscription cancelSubscription =
                SubscribeDataEventRequest.CancelSubscription.newBuilder()
                .build();

        return SubscribeDataEventRequest.newBuilder()
                .setCancelSubscription(cancelSubscription)
                .build();
    }

}
