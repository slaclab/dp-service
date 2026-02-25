package com.ospreydcs.dp.service.query.handler.mongo;

import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.SamplingClock;
import com.ospreydcs.dp.grpc.v1.query.QueryDataRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryDataResponse;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketUtility;
import com.ospreydcs.dp.service.common.mongo.MongoClientBase;
import com.ospreydcs.dp.service.query.QueryTestBase;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.QueryDataBidiStreamDispatcher;
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.QueryDataStreamDispatcher;
import com.ospreydcs.dp.service.query.handler.mongo.job.QueryDataJob;
import io.grpc.stub.StreamObserver;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MongoQueryHandlerTestBase extends QueryTestBase {

    protected static MongoQueryHandler handler = null;
    protected static TestClientInterface clientTestInterface = null;
    private static String collectionNamePrefix = null;
    protected static final long startSeconds = Instant.now().getEpochSecond();
    protected static final String columnNameBase = "testpv_";
    protected static final String COL_1_NAME = columnNameBase + "1";
    protected static final String COL_2_NAME = columnNameBase + "2";



    protected interface TestClientInterface extends MongoQueryClientInterface {
        public int insertBucketDocuments(List<BucketDocument> documentList);
    }

    private static String getTestCollectionNamePrefix() {
        if (collectionNamePrefix == null) {
            collectionNamePrefix = "test-" + System.currentTimeMillis() + "-";
        }
        return collectionNamePrefix;
    }

    protected static String getTestCollectionNameBuckets() {
        return getTestCollectionNamePrefix() + MongoClientBase.COLLECTION_NAME_BUCKETS;
    }

    protected static String getTestCollectionNameRequestStatus() {
        return getTestCollectionNamePrefix() + MongoClientBase.COLLECTION_NAME_REQUEST_STATUS;
    }

    public static void setUp(MongoQueryHandler handler, TestClientInterface clientInterface) throws Exception {
        System.out.println("setUp");
        MongoQueryHandlerTestBase.handler = handler;
        clientTestInterface = clientInterface;
        assertTrue(clientTestInterface.init());

        // create test data, 5 pvs sampled 10 times per second, 10 buckets per pv each with one second's data
        final int numSamplesPerSecond = 10;
        final int numSecondsPerBucket = 1;
        final int numColumns = 5;
        final int numBucketsPerColumn = 10;
        List<BucketDocument> bucketList = BucketUtility.createBucketDocuments(
                startSeconds, numSamplesPerSecond, numSecondsPerBucket, columnNameBase, numColumns, numBucketsPerColumn);
        assertEquals(clientTestInterface.insertBucketDocuments(bucketList), bucketList.size());
    }

    public static void tearDown() throws Exception {
        System.out.println("tearDown");
        assertTrue(clientTestInterface.fini());
        handler = null;
        clientTestInterface = null;
    }

    protected List<QueryDataResponse> executeAndDispatchResponseStream(QueryDataRequest request) {

        List<QueryDataResponse> responseList = new ArrayList<>();

        // create observer for api response stream
        StreamObserver<QueryDataResponse> responseObserver = new StreamObserver<QueryDataResponse>() {

            @Override
            public void onNext(QueryDataResponse queryDataResponse) {
                System.out.println("responseObserver.onNext");
                responseList.add(queryDataResponse);
            }

            @Override
            public void onError(Throwable throwable) {
                System.out.println("responseObserver.onError");
            }

            @Override
            public void onCompleted() {
                System.out.println("responseObserver.Completed");
            }
        };

        // create QueryJob and execute it
        final QueryDataStreamDispatcher dispatcher =
                new QueryDataStreamDispatcher(responseObserver, request.getQuerySpec());
        final QueryDataJob job =
                new QueryDataJob(request.getQuerySpec(), dispatcher, responseObserver, clientTestInterface);
        job.execute();

        return responseList;
    }

    private class ResponseCursorStreamObserver implements StreamObserver<QueryDataResponse> {

        private QueryDataBidiStreamDispatcher dispatcher = null;
        final CountDownLatch finishLatch;
        final List<QueryDataResponse> responseList;

        public ResponseCursorStreamObserver(CountDownLatch finishLatch, List<QueryDataResponse> responseList) {
            this.finishLatch = finishLatch;
            this.responseList = responseList;
        }

        public void setDispatcher(QueryDataBidiStreamDispatcher dispatcher) {
            this.dispatcher = dispatcher;
        }

        @Override
        public void onNext(QueryDataResponse queryDataResponse) {
            System.out.println("responseObserver.onNext");
            if (queryDataResponse.hasExceptionalResult()) {
                System.out.println("exceptional response message: "
                        + queryDataResponse.getExceptionalResult().getMessage());
                finishLatch.countDown();
            } else {
                System.out.println("adding detail response");
                responseList.add(queryDataResponse);
//                this.dispatcher.next();
            }
        }

        @Override
        public void onError(Throwable throwable) {
            System.out.println("responseObserver.onError");
            finishLatch.countDown();
        }

        @Override
        public void onCompleted() {
            System.out.println("responseObserver.onCompleted");
            finishLatch.countDown();
        }
    }

    protected List<QueryDataResponse> executeAndDispatchResponseCursor(QueryDataRequest request) {

        List<QueryDataResponse> responseList = new ArrayList<>();
        final CountDownLatch finishLatch = new CountDownLatch(1);

        // create observer for api response stream
        ResponseCursorStreamObserver responseObserver = new ResponseCursorStreamObserver(finishLatch, responseList);

        // create QueryJob and execute it
        final QueryDataBidiStreamDispatcher dispatcher =
                new QueryDataBidiStreamDispatcher(responseObserver, request.getQuerySpec());
        responseObserver.setDispatcher(dispatcher);
        final QueryDataJob job =
                new QueryDataJob(request.getQuerySpec(), dispatcher, responseObserver, clientTestInterface);
        job.execute();

        // check if RPC already completed
        if (finishLatch.getCount() == 0) {
            // RPC completed or errored already
            return responseList;
        }

        // otherwise wait for completion
        try {
            boolean awaitSuccess = finishLatch.await(1, TimeUnit.MINUTES);
            if (!awaitSuccess) {
                System.out.println("timeout waiting for finishLatch");
                return responseList;
            }
        } catch (InterruptedException e) {
            System.out.println("InterruptedException waiting for finishLatch");
            return responseList;
        }

        return responseObserver.responseList;
    }

    private static void verifyDataBucket(
            DataBucket bucket,
            long bucketStartSeconds,
            long bucketStartNanos,
            long bucketSampleIntervalNanos,
            int bucketNumSamples,
            String bucketColumnName) {

        assertTrue(bucket.hasDataTimestamps());
        assertTrue(bucket.getDataTimestamps().hasSamplingClock());
        SamplingClock samplingClock = bucket.getDataTimestamps().getSamplingClock();
        assertEquals(samplingClock.getStartTime().getEpochSeconds(), bucketStartSeconds);
        assertEquals(samplingClock.getStartTime().getNanoseconds(), bucketStartNanos);
        assertEquals(samplingClock.getPeriodNanos(), bucketSampleIntervalNanos);
        assertEquals(samplingClock.getCount(), bucketNumSamples);
        assertTrue(bucket.hasDataColumn());
        DataColumn bucketColumn = bucket.getDataColumn();
        assertEquals(bucketColumn.getName(), bucketColumnName);
        assertEquals(bucketColumn.getDataValuesCount(), bucketNumSamples);
        for (int valueIndex = 0 ; valueIndex < bucketColumn.getDataValuesCount() ; valueIndex++) {
            DataValue dataValue = bucketColumn.getDataValues(valueIndex);
            assertEquals((double) valueIndex, dataValue.getDoubleValue(), 0);
        }
    }

    private static void verifyQueryResponse(List<QueryDataResponse> responseList) {

        // examine response
        final int numResponesesExpected = 1;
        assertEquals(numResponesesExpected, responseList.size());

        // check data message
        final int numSamplesPerBucket = 10;
        final long bucketSampleIntervalNanos = 100_000_000L;
        QueryDataResponse dataResponse = responseList.get(0);
        assertTrue(dataResponse.hasQueryData());
        QueryDataResponse.QueryData queryData = dataResponse.getQueryData();
        assertEquals(numSamplesPerBucket, queryData.getDataBucketsCount());
        List<DataBucket> bucketList = queryData.getDataBucketsList();

        // check each bucket, 5 for each column
        int secondsIncrement = 0;
        String columnName = COL_1_NAME;
        for (int bucketIndex = 0 ; bucketIndex < 10 ; bucketIndex++) {
            final long bucketStartSeconds = startSeconds + secondsIncrement;
            verifyDataBucket(
                    bucketList.get(bucketIndex),
                    bucketStartSeconds,
                    0,
                    bucketSampleIntervalNanos,
                    numSamplesPerBucket,
                    columnName);
            secondsIncrement = secondsIncrement + 1;
            if (bucketIndex == 4) {
                secondsIncrement = 0;
                columnName = COL_2_NAME;
            }
        }
    }

    public void testResponseStreamDispatcher() {

        // assemble query request
        List<String> columnNames = List.of(COL_1_NAME, COL_2_NAME);
        QueryDataRequestParams params = new QueryDataRequestParams(
                columnNames,
                startSeconds,
                0L,
                startSeconds + 5,
                0L);
        QueryDataRequest request = buildQueryDataRequest(params);

        // execute query and dispatch result using ResponseStreamDispatcher
        List<QueryDataResponse> responseList = executeAndDispatchResponseStream(request);

        // examine response
        verifyQueryResponse(responseList);
    }

    public void testResponseCursorDispatcher() {

        // assemble query request
        List<String> columnNames = List.of(COL_1_NAME, COL_2_NAME);
        QueryDataRequestParams params = new QueryDataRequestParams(
                columnNames,
                startSeconds,
                0L,
                startSeconds + 5,
                0L);
        QueryDataRequest request = buildQueryDataRequest(params);

        // execute query and dispatch using ResponseCursorDispatcher
        List<QueryDataResponse> responseList = executeAndDispatchResponseCursor(request);

        // examine response
        verifyQueryResponse(responseList);
    }

}
