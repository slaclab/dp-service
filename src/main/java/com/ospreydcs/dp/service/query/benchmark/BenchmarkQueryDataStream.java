package com.ospreydcs.dp.service.query.benchmark;

import com.ospreydcs.dp.grpc.v1.query.DpQueryServiceGrpc;
import com.ospreydcs.dp.grpc.v1.query.QueryDataRequest;
import io.grpc.Channel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class BenchmarkQueryDataStream extends QueryBenchmarkBase {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    public static class QueryResponseStreamTask extends QueryDataResponseTask {

        public QueryResponseStreamTask(Channel channel, QueryDataRequestTaskParams params) {
            super(channel, params);
        }

        public QueryTaskResult call() {
            QueryTaskResult result = sendQueryResponseStream(this.channel, this.params);
            return result;
        }

        private QueryTaskResult sendQueryResponseStream(
                Channel channel,
                QueryDataRequestTaskParams params) {

            final int streamNumber = params.streamNumber();
            final CountDownLatch finishLatch = new CountDownLatch(1);

            boolean success = true;
            String msg = "";
            long dataValuesReceived = 0;
            long dataBytesReceived = 0;
            long grpcBytesReceived = 0;
            int numBucketsReceived = 0;
            int numResponsesReceived = 0;

            // build query request
            final QueryDataRequest request = buildQueryDataRequest(params);

            // call hook for subclasses to validate request
            try {
                onRequest(request);
            } catch (AssertionError assertionError) {
                System.err.println("stream: " + streamNumber + " assertion error");
                assertionError.printStackTrace(System.err);
                return new QueryTaskResult(false, 0, 0, 0);
            }

            // create observer for api response stream
            final QueryDataResponseObserver responseObserver =
                    new QueryDataResponseObserver(streamNumber, params, finishLatch, this);

            // create observer for api request stream and invoke api
            final DpQueryServiceGrpc.DpQueryServiceStub asyncStub = DpQueryServiceGrpc.newStub(channel);
            asyncStub.queryDataStream(request, responseObserver);

            // wait for completion of response stream
            try {
                boolean awaitSuccess = finishLatch.await(AWAIT_TIMEOUT_MINUTES, TimeUnit.MINUTES);
                if (!awaitSuccess) {
                    logger.error("stream: {} timeout waiting for finishLatch", streamNumber);
                    return new QueryTaskResult(false, 0, 0, 0);
                }
            } catch (InterruptedException e) {
                logger.error("stream: {} InterruptedException waiting for finishLatch", streamNumber);
                return new QueryTaskResult(false, 0, 0, 0);
            }

            boolean taskError = responseObserver.isError.get();

            if (!taskError) {

                // call hook for subclasses to add validation
                try {
                    onCompleted();
                } catch (AssertionError assertionError) {
                    System.err.println("stream: " + streamNumber + " assertion error");
                    assertionError.printStackTrace(System.err);
                    return new QueryTaskResult(false, 0, 0, 0);
                }

                return new QueryTaskResult(
                        true,
                        responseObserver.dataValuesReceived.get(),
                        responseObserver.dataBytesReceived.get(),
                        responseObserver.grpcBytesReceived.get());

            } else {
                return new QueryTaskResult(false, 0, 0, 0);
            }
        }

    }

    protected QueryResponseStreamTask newQueryTask(Channel channel, QueryDataRequestTaskParams params) {
        return new QueryResponseStreamTask(channel, params);
    }

    public static void main(final String[] args) {

//        final int[] totalNumPvsArray = {100, 500, 1000};
//        final int[] numPvsPerRequestArray = {1, 10, 25, 50};
//        final int[] numThreadsArray = {1, 3, 5, 7};

        final int[] totalNumPvsArray = {1000};
        final int[] numPvsPerRequestArray = {10};
        final int[] numThreadsArray = {5};

        BenchmarkQueryDataStream benchmark = new BenchmarkQueryDataStream();
        runBenchmark(benchmark, totalNumPvsArray, numPvsPerRequestArray, numThreadsArray);
    }

}
