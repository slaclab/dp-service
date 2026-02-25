package com.ospreydcs.dp.service.integration.query;

import com.ospreydcs.dp.service.integration.GrpcIntegrationTestBase;
import com.ospreydcs.dp.service.integration.ingest.GrpcIntegrationIngestionServiceWrapper;
import com.ospreydcs.dp.service.query.QueryTestBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RunWith(JUnit4.class)
public class QueryDataStreamStaggeredTimestampIT extends GrpcIntegrationTestBase {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // constants
    private static final int INGESTION_PROVIDER_ID = 1;
    public static final String CFG_KEY_START_SECONDS = "IngestionBenchmark.startSeconds";
    public static final Long DEFAULT_START_SECONDS = 1698767462L;

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void staggeredTimestampTest() {

        final long startSeconds = configMgr().getConfigLong(CFG_KEY_START_SECONDS, DEFAULT_START_SECONDS);
        final long startNanos = 0L;
        final String columnNameTenths = "TEST_TENTHS";
        final String columnNameEighths = "TEST_EIGHTHS";
        final String columnNameFifths = "TEST_FIFTHS";
        final String columnNameQuarters = "TEST_QUARTERS";

        // register ingestion provider
        final String providerName = String.valueOf(INGESTION_PROVIDER_ID);
        final String providerId = ingestionServiceWrapper.registerProvider(providerName, null);

        List<GrpcIntegrationIngestionServiceWrapper.IngestionColumnInfo> columnInfoList = new ArrayList<>();

        {
            // ingest data with timestamps every tenth of a second
            final String requestIdBaseTenths = "TEN-";
            final long intervalTenths = 100_000_000L;
            final int numBucketsTenths = 30;
            final int numSecondsPerBucketTenths = 1;
            final GrpcIntegrationIngestionServiceWrapper.IngestionColumnInfo columnInfoTenths =
                    new GrpcIntegrationIngestionServiceWrapper.IngestionColumnInfo(
                            columnNameTenths,
                            requestIdBaseTenths,
                            providerId,
                            intervalTenths,
                            numBucketsTenths,
                            numSecondsPerBucketTenths, 
                            false,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null);
            columnInfoList.add(columnInfoTenths);
        }

        {
            // ingest data with timestamps every fifth of a second
            final String requestIdBaseFifths = "FIFTH-";
            final long intervalFifths = 200_000_000L;
            final int numBucketsFifths = 10;
            final int numSecondsPerBucketFifths = 3;
            final GrpcIntegrationIngestionServiceWrapper.IngestionColumnInfo columnInfoFifths =
                    new GrpcIntegrationIngestionServiceWrapper.IngestionColumnInfo(
                            columnNameFifths,
                            requestIdBaseFifths,
                            providerId,
                            intervalFifths,
                            numBucketsFifths,
                            numSecondsPerBucketFifths,
                            false,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null);
            columnInfoList.add(columnInfoFifths);
        }

        {
            // ingest data with timestamps every quarter of a second
            final String requestIdBaseQuarters = "QUARTER-";
            final long intervalQuarters = 250_000_000L;
            final int numBucketsQuarters = 6;
            final int numSecondsPerBucketQuarters = 5;
            final GrpcIntegrationIngestionServiceWrapper.IngestionColumnInfo columnInfoQuarters =
                    new GrpcIntegrationIngestionServiceWrapper.IngestionColumnInfo(
                            columnNameQuarters,
                            requestIdBaseQuarters,
                            providerId,
                            intervalQuarters,
                            numBucketsQuarters,
                            numSecondsPerBucketQuarters, 
                            false,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null);
            columnInfoList.add(columnInfoQuarters);
        }

        {
            // ingest data with timestamps every eighth of a second, using explicit TimestampsList instead of SamplingClock
            final String requestIdBaseEighths = "EIGHTH-";
            final long intervalEighths = 125_000_000L;
            final int numBucketsEighths = 16;
            final int numSecondsPerBucketEighths = 2;
            final GrpcIntegrationIngestionServiceWrapper.IngestionColumnInfo columnInfoEighths =
                    new GrpcIntegrationIngestionServiceWrapper.IngestionColumnInfo(
                            columnNameEighths,
                            requestIdBaseEighths,
                            providerId,
                            intervalEighths,
                            numBucketsEighths,
                            numSecondsPerBucketEighths,
                            true,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null); // specify that DataTimestamps in request should use explicit TimestampsList
            columnInfoList.add(columnInfoEighths);
        }

        Map<String, GrpcIntegrationIngestionServiceWrapper.IngestionStreamInfo> validationMap = null;
        {
            // perform ingestion for specified list of columns
            validationMap = ingestionServiceWrapper.ingestDataBidiStreamFromColumn(
                    columnInfoList, startSeconds, startNanos);
        }

        {
            // send table query for 5-second subset of ingested data,
            // starting one second offset from start of ingestion data
            final int queryNumSeconds = 5;
            final long queryStartSeconds = startSeconds + 1;
            final long queryStartNanos = 0;
            final long queryEndSeconds = queryStartSeconds + queryNumSeconds;
            final long queryEndNanos = 0;
            final List<String> queryColumnNames = List.of(
                    columnNameTenths, columnNameFifths, columnNameQuarters, columnNameEighths);

            final int numRowsExpected = 16 * queryNumSeconds;

            queryServiceWrapper.sendAndVerifyQueryTablePvNameListColumnResult(
                    numRowsExpected,
                    queryColumnNames,
                    queryStartSeconds,
                    queryStartNanos,
                    queryEndSeconds,
                    queryEndNanos,
                    validationMap, false, "");
        }

        {
            // send bucket query for 5-second subset of ingested data,
            // starting one second offset from start of ingestion data.
            final List<String> queryColumnNamesBucket =
                    List.of(columnNameTenths, columnNameFifths, columnNameQuarters, columnNameEighths);
            final int queryNumSecondsBucket = 5;
            final long queryStartSecondsBucket = startSeconds + 1;
            final long queryStartNanosBucket = 0;
            final long queryEndSecondsBucket = queryStartSecondsBucket + queryNumSecondsBucket;
            final long queryEndNanosBucket = 0;

            // we expect 5 buckets for tenths (1 secs/bucket),
            // 3 buckets for eights (2 secs/bucket),
            // 2 buckets for fifths (3 secs/bucket),
            // 2 buckets for quarters (5 secs/bucket)
            final int numBucketsExpected = 12;

            final QueryTestBase.QueryDataRequestParams params = new QueryTestBase.QueryDataRequestParams(
                    queryColumnNamesBucket,
                    queryStartSecondsBucket,
                    queryStartNanosBucket,
                    queryEndSecondsBucket,
                    queryEndNanosBucket
            );

            queryServiceWrapper.sendAndVerifyQueryDataStream(
                    numBucketsExpected,
                    params,
                    validationMap,
                    false,
                    "");
        }
    }

}
