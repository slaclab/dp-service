package com.ospreydcs.dp.service.query.handler.mongo;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.common.ExceptionalResult;
import com.ospreydcs.dp.grpc.v1.query.QueryDataRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryDataResponse;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.mongo.MongoTestClient;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoSyncQueryClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class MongoQueryHandlerErrorTest extends MongoQueryHandlerTestBase {

    protected static class ErrorTestClient extends MongoSyncQueryClient implements TestClientInterface {

        @Override
        protected String getCollectionNameBuckets() {
            // THIS TEST WILL create an empty mongo collection, unfortunately.
            return getTestCollectionNameBuckets();
        }

        @Override
        protected String getCollectionNameRequestStatus() {
            return getTestCollectionNameRequestStatus();
        }

        public int insertBucketDocuments(List<BucketDocument> documentList) {
            // DOESN'T ACTUALLY INSERT DATA to mongo, we don't need it for these tests.
            return documentList.size();
        }

        @Override
        public MongoCursor<BucketDocument> executeQueryData(QueryDataRequest.QuerySpec querySpec) {
            // THIS IS KEY FOR THE TEST CASE.
            // THE NULL CURSOR returned by this method triggers the error response condition from the handler!
            return null;
        }
    }

    @BeforeClass
    public static void setUp() throws Exception {

        // Use test db client to set database name globally to "dp-test" and remove that database if it already exists
        MongoTestClient.prepareTestDatabase();

        ErrorTestClient testClient = new ErrorTestClient();
        MongoQueryHandler handler = new MongoQueryHandler(testClient);
        setUp(handler, testClient);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        MongoQueryHandlerTestBase.tearDown();
    }

    @Test
    public void testProcessQueryRequestCursorError() {

        // assemble query request
        String col1Name = columnNameBase + "1";
        String col2Name = columnNameBase + "2";
        List<String> columnNames = List.of(col1Name, col2Name);
        QueryDataRequestParams params = new QueryDataRequestParams(
                columnNames,
                startSeconds,
                0L,
                startSeconds + 5,
                0L);
        QueryDataRequest request = buildQueryDataRequest(params);

        // send request
        final int numResponesesExpected = 1;

        // WE EXPECT processQueryRequest to trigger an error response from the handler since
        // ErrorTestClient.executeQuery RETURNS A NULL CURSOR.
        List<QueryDataResponse> responseList = executeAndDispatchResponseStream(request);

        // examine response
        assertEquals(numResponesesExpected, responseList.size());
        QueryDataResponse summaryResponse = responseList.get(0);
        assertTrue(summaryResponse.hasExceptionalResult());
        assertEquals(
                ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR,
                summaryResponse.getExceptionalResult().getExceptionalResultStatus());
        assertEquals("executeQuery returned null cursor", summaryResponse.getExceptionalResult().getMessage());
    }

}
