package com.ospreydcs.dp.service.query.benchmark;

import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.query.QueryDataResponse;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class QueryDataResponseObserver implements StreamObserver<QueryDataResponse> {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    final private int streamNumber;
    final private QueryBenchmarkBase.QueryDataRequestTaskParams params;
    final public CountDownLatch finishLatch;
    final private QueryBenchmarkBase.QueryDataResponseTask task;
    protected AtomicBoolean isError = new AtomicBoolean(false);
    protected AtomicInteger dataValuesReceived = new AtomicInteger(0);
    protected AtomicInteger dataBytesReceived = new AtomicInteger(0);
    protected AtomicInteger grpcBytesReceived = new AtomicInteger(0);
    private AtomicInteger numResponsesReceived = new AtomicInteger(0);
    private AtomicInteger numBucketsReceived = new AtomicInteger(0);

    public QueryDataResponseObserver(
            int streamNumber,
            QueryBenchmarkBase.QueryDataRequestTaskParams params,
            CountDownLatch finishLatch,
            QueryBenchmarkBase.QueryDataResponseTask task
    ) {
        this.streamNumber = streamNumber;
        this.params = params;
        this.finishLatch = finishLatch;
        this.task = task;
    }

    protected void verifyResponse(QueryDataResponse response) {
        task.onResponse(response);
    }

    protected void onAssertionError(AssertionError assertionError) {
        // empty override
    }

    protected void onAdditionalBuckets() {
        // empty override
    }

    @Override
    public void onNext(QueryDataResponse response) {

        if (finishLatch.getCount() == 0) {
            return;
        }

        logger.trace("stream: {} received response type: {}", streamNumber, response.getResultCase().toString());

        boolean success = true;
        String msg = "";

        if (response.hasExceptionalResult()) {
            isError.set(true);
            success = false;
            msg = "stream: " + streamNumber
                    + " received exception with message: " + response.getExceptionalResult().getMessage();
            logger.error(msg);

        } else if (response.hasQueryData()) {

            grpcBytesReceived.getAndAdd(response.getSerializedSize());
            numResponsesReceived.incrementAndGet();

            QueryDataResponse.QueryData queryData = response.getQueryData();
            int numResultBuckets = queryData.getDataBucketsCount();
            logger.trace("stream: {} received data result numBuckets: {}", streamNumber, numResultBuckets);

            for (DataBucket bucket : queryData.getDataBucketsList()) {

                int dataValuesCount = 0;
                if (bucket.getDataValues().hasDataColumn()) {
                    dataValuesCount = bucket.getDataValues().getDataColumn().getDataValuesCount();

                } else if (bucket.getDataValues().hasSerializedDataColumn()) {

                    if (bucket.getDataTimestamps().hasSamplingClock()) {
                        dataValuesCount = bucket.getDataTimestamps().getSamplingClock().getCount();

                    } else if (bucket.getDataTimestamps().hasTimestampList()) {
                        dataValuesCount = bucket.getDataTimestamps().getTimestampList().getTimestampsCount();
                    }
                }

                dataValuesReceived.addAndGet(dataValuesCount);
                dataBytesReceived.addAndGet(dataValuesCount * Double.BYTES);
                numBucketsReceived.incrementAndGet();
            }

            // call hook for subclasses to add validation
            try {
                verifyResponse(response);
            } catch (AssertionError assertionError) {
                if (finishLatch.getCount() > 0) {
                    System.err.println("stream: " + streamNumber + " assertion error");
                    assertionError.printStackTrace(System.err);
                    isError.set(true);
                    finishLatch.countDown();
                    onAssertionError(assertionError);

                }
                return;
            } catch (Exception e) {
                System.err.println("stream: " + streamNumber + " exception: " + e.getMessage());
                e.printStackTrace(System.err);
                isError.set(true);
                finishLatch.countDown();
            }

        } else {
            isError.set(true);
            success = false;
            msg = "stream: " + streamNumber + " received unexpected response";
            logger.error(msg);
        }

        if (success) {

            final int numBucketsExpected = params.columnNames().size() * params.numSeconds();
            final int numBucketsReceivedValue = numBucketsReceived.get();

            if  ( numBucketsReceivedValue < numBucketsExpected) {
                onAdditionalBuckets();

            } else {
                // otherwise signal that we are done
                logger.trace("stream: {} onNext received expected number of buckets: {}",
                        streamNumber, numBucketsExpected);
                finishLatch.countDown();
            }

        } else {
            // something went wrong, signal that we are done
            isError.set(true);
            logger.error("stream: {} onNext unexpected error", streamNumber);
            finishLatch.countDown();
        }

    }

    @Override
    public void onError(Throwable t) {
        logger.error("stream: {} responseObserver.onError with msg: {} cause: {}",
                streamNumber, t.getMessage(), t.getCause().getMessage());
        isError.set(true);
        if (finishLatch.getCount() > 0) {
            finishLatch.countDown();
        }
    }

    @Override
    public void onCompleted() {
        logger.trace("stream: {} responseObserver.onCompleted", streamNumber);
    }

}
