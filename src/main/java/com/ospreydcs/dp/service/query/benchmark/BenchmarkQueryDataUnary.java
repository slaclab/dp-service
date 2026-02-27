package com.ospreydcs.dp.service.query.benchmark;

import com.ospreydcs.dp.grpc.v1.query.DpQueryServiceGrpc;
import com.ospreydcs.dp.grpc.v1.query.QueryDataRequest;
import io.grpc.Channel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class BenchmarkQueryDataUnary extends QueryBenchmarkBase {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    public static class QueryResponseSingleTask extends QueryDataResponseTask {

        public QueryResponseSingleTask(Channel channel, QueryDataRequestTaskParams params) {
            super(channel, params);
        }

        public QueryTaskResult call() {
            QueryTaskResult result = sendQueryResponseSingle(this.channel, this.params);
            return result;
        }

        private QueryTaskResult sendQueryResponseSingle(
                Channel channel,
                QueryDataRequestTaskParams params) {

            final int streamNumber = params.streamNumber();
            final CountDownLatch finishLatch = new CountDownLatch(1);

            QueryDataRequest request = buildQueryDataRequest(params);

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

            // invoke api
            final DpQueryServiceGrpc.DpQueryServiceStub asyncStub = DpQueryServiceGrpc.newStub(channel);
            asyncStub.queryData(request, responseObserver);

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

    protected QueryResponseSingleTask newQueryTask(Channel channel, QueryDataRequestTaskParams params) {
        return new QueryResponseSingleTask(channel, params);
    }

    public static void main(final String[] args) {

        final int[] totalNumPvsArray = {10, /*100, 500, 1000*/}; // use small number of PVs so we don't get errors for exceeding message size limit for unary response
        final int[] numPvsPerRequestArray = {/*1,*/ 1/*, 25, 50*/};
        final int[] numThreadsArray = {/*1, 3,*/ 5/*, 7*/};

        BenchmarkQueryDataUnary benchmark = new BenchmarkQueryDataUnary();
        runBenchmark(benchmark, totalNumPvsArray, numPvsPerRequestArray, numThreadsArray);
    }

}
