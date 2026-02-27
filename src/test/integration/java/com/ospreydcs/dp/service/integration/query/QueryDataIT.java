package com.ospreydcs.dp.service.integration.query;

import com.ospreydcs.dp.service.integration.GrpcIntegrationTestBase;
import com.ospreydcs.dp.service.integration.ingest.GrpcIntegrationIngestionServiceWrapper;
import com.ospreydcs.dp.service.query.QueryTestBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.time.Instant;
import java.util.List;

@RunWith(JUnit4.class)
public class QueryDataIT extends GrpcIntegrationTestBase {

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
    public void testQueryData() {

        // use request data contained by validationMap to verify query results
        final long startSeconds = Instant.now().getEpochSecond();
        GrpcIntegrationIngestionServiceWrapper.IngestionScenarioResult ingestionScenarioResult;
        {
            // create some data for testing query APIs
            // create data for 10 sectors, each containing 3 gauges and 3 bpms
            // named with prefix "S%02d-" followed by "GCC%02d" or "BPM%02d"
            // with 10 measurements per bucket, 1 bucket per second, and 10 buckets per pv
            ingestionScenarioResult = ingestionServiceWrapper.simpleIngestionScenario(startSeconds, false);
        }

        // positive queryData() test case, empty query result
        {
            final List<String> pvNames = List.of("junk", "stuff"); // bogus PV names

            // select 5 seconds of data for each pv
            final long beginSeconds = startSeconds + 1;
            final long beginNanos = 0L;
            final long endSeconds = startSeconds + 6;
            final long endNanos = 0L;

            final int numBucketsExpected = 0; // we expect an empty response
            final boolean expectReject = false;
            final String expectedRejectMessage = "";

            final QueryTestBase.QueryDataRequestParams params =
                    new QueryTestBase.QueryDataRequestParams(pvNames,
                            beginSeconds,
                            beginNanos,
                            endSeconds,
                            endNanos
                    );

            queryServiceWrapper.sendAndVerifyQueryData(
                    numBucketsExpected,
                    params,
                    ingestionScenarioResult.validationMap(),
                    expectReject,
                    expectedRejectMessage
            );
        }

        // positive queryData() test case
        {
            final List<String> pvNames = List.of("S01-GCC01", "S01-BPM01");

            // select 5 seconds of data for each pv
            final long beginSeconds = startSeconds + 1;
            final long beginNanos = 0L;
            final long endSeconds = startSeconds + 6;
            final long endNanos = 0L;

            // 2 pvs, 5 seconds, 1 bucket per second per pv
            final int numBucketsExpected = 10;
            final boolean expectReject = false;
            final String expectedRejectMessage = "";

            final QueryTestBase.QueryDataRequestParams params =
                    new QueryTestBase.QueryDataRequestParams(pvNames,
                            beginSeconds,
                            beginNanos,
                            endSeconds,
                            endNanos
                    );

            queryServiceWrapper.sendAndVerifyQueryData(
                    numBucketsExpected,
                    params,
                    ingestionScenarioResult.validationMap(),
                    expectReject,
                    expectedRejectMessage
            );
        }
    }

}
