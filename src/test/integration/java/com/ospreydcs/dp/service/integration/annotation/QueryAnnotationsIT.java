package com.ospreydcs.dp.service.integration.annotation;

import com.ospreydcs.dp.grpc.v1.annotation.DataBlock;
import com.ospreydcs.dp.grpc.v1.annotation.DataSet;
import com.ospreydcs.dp.grpc.v1.annotation.QueryAnnotationsResponse;
import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.service.annotation.AnnotationTestBase;
import com.ospreydcs.dp.service.query.QueryTestBase;
import org.junit.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QueryAnnotationsIT extends AnnotationIntegrationTestIntermediate {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void testQueryAnnotationsNegative() {

        // queryAnnotations() negative test: empty annotationId in query by IdCriterion
        {
            final String blankAnnotationId = "";
            final AnnotationTestBase.QueryAnnotationsParams queryParams =
                    new AnnotationTestBase.QueryAnnotationsParams();
            queryParams.setIdCriterion(blankAnnotationId);

            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "QueryAnnotationsRequest.criteria.IdCriterion id must be specified";

            annotationServiceWrapper.sendAndVerifyQueryAnnotations(
                    queryParams,
                    expectReject,
                    expectedRejectMessage,
                    new ArrayList<>());
        }

        // queryAnnotations() negative test: empty comment text in query by OwnerCriterion and TextCriterion
        {
            final String ownerId = "craigmcc";
            final String blankCommentText = "";
            final AnnotationTestBase.QueryAnnotationsParams queryParams =
                    new AnnotationTestBase.QueryAnnotationsParams();
            queryParams.setOwnerCriterion(ownerId);
            queryParams.setTextCriterion(blankCommentText);

            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "QueryAnnotationsRequest.criteria.TextCriterion text must be specified";

            annotationServiceWrapper.sendAndVerifyQueryAnnotations(
                    queryParams,
                    expectReject,
                    expectedRejectMessage,
                    new ArrayList<>());
        }

        // queryAnnotations() negative test: empty datasetId in query by OwnerCriterion and DataSetCriterion.
        {
            final String ownerId = "craigmcc";
            final String blankDatasetId = "";
            final AnnotationTestBase.QueryAnnotationsParams queryParams =
                    new AnnotationTestBase.QueryAnnotationsParams();
            queryParams.setOwnerCriterion(ownerId);
            queryParams.setDatasetsCriterion(blankDatasetId);

            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "QueryAnnotationsRequest.criteria.DataSetCriterion dataSetId must be specified";

            annotationServiceWrapper.sendAndVerifyQueryAnnotations(
                    queryParams,
                    expectReject,
                    expectedRejectMessage,
                    new ArrayList<>());
        }

    }

    @Test
    public void testQueryAnnotationsPositive() {

        final long startSeconds = Instant.now().getEpochSecond();
        final long startNanos = 0L;

        // run ingestion, create datasets and annotations needed for tests
        annotationIngestionScenario(startSeconds);
        CreateDataSetScenarioResult createDataSetScenarioResult = createDataSetScenario(startSeconds);
        CreateAnnotationScenarioResult createAnnotationScenarioResult = createAnnotationScenario(
                startSeconds,
                createDataSetScenarioResult.firstHalfDataSetId(),
                createDataSetScenarioResult.secondHalfDataSetId());

        // queryAnnotations() negative test: empty query result.
        {
            final String unknownText = "JUNK";
            final AnnotationTestBase.QueryAnnotationsParams queryParams =
                    new AnnotationTestBase.QueryAnnotationsParams();
            queryParams.setTextCriterion(unknownText);

            final boolean expectReject = false;
            final String expectedRejectMessage ="";

            annotationServiceWrapper.sendAndVerifyQueryAnnotations(
                    queryParams,
                    expectReject,
                    expectedRejectMessage,
                    new ArrayList<>());
        }

        // queryAnnotations() positive test for query by OwnerCriterion and DataSetCriterion.
        {
            /*
             * This test scenario utilizes the annotations created above, which include 10 annotations for each of two
             * different owners, with 5 annotations for a dataset with blocks for the first half second of a 5 second
             * interval, and 5 annotations for the second half second of that interval.
             *
             * The queryAnnotations() test will retrieve annotations for one of the owners for the first half data set,
             * and confirm that only the appropriate 5 annotations are retrieved.
             */

            final String ownerId = "craigmcc";
            final String datasetId = createDataSetScenarioResult.firstHalfDataSetId();
            final AnnotationTestBase.QueryAnnotationsParams queryParams =
                    new AnnotationTestBase.QueryAnnotationsParams();
            queryParams.setOwnerCriterion(ownerId);
            queryParams.setDatasetsCriterion(datasetId);

            final boolean expectReject = false;
            final String expectedRejectMessage ="";

            annotationServiceWrapper.sendAndVerifyQueryAnnotations(
                    queryParams,
                    expectReject,
                    expectedRejectMessage,
                    createAnnotationScenarioResult.firstHalfAnnotationsOwnerCraigmcc());
        }

        // queryAnnotations() positive test for query by OwnerCriterion and TextCriterion (over comment field).
        List<QueryAnnotationsResponse.AnnotationsResult.Annotation> annotationsQueryResult = null;
        {
            /*
             * This test scenario utilizes the annotations created above, which include 10 annotations for each of two
             * different owners, with 5 annotations for a dataset with blocks for the first half second of a 5 second
             * interval, and 5 annotations for the second half second of that interval.
             *
             * The queryAnnotations() test will retrieve annotations for one of the owners for the first half data set,
             * and confirm that only the appropriate 5 annotations are retrieved.
             */

            final String ownerId = "craigmcc";
            final String commentText = "first";
            final AnnotationTestBase.QueryAnnotationsParams queryParams =
                    new AnnotationTestBase.QueryAnnotationsParams();
            queryParams.setOwnerCriterion(ownerId);
            queryParams.setTextCriterion(commentText);

            final boolean expectReject = false;
            final String expectedRejectMessage ="";

            annotationsQueryResult = annotationServiceWrapper.sendAndVerifyQueryAnnotations(
                    queryParams,
                    expectReject,
                    expectedRejectMessage,
                    createAnnotationScenarioResult.firstHalfAnnotationsOwnerCraigmcc());

            // query data test using result of queryAnnotations()
            {
                /*
                 * This test scenario uses the result from queryAnnotations to send a data query for one of the datasets.
                 * We iterate through each annoation from the query result, and send a queryDataStream() data query for each
                 * data block in the annotation's dataset, verifying that we receive the buckets expected for the specified
                 * pvNames and that each bucket has the expected begin time.
                 */

                for (QueryAnnotationsResponse.AnnotationsResult.Annotation resultAnnotation : annotationsQueryResult) {

                    for (DataSet resultDataSet : resultAnnotation.getDataSetsList()) {

                        for (DataBlock queryResultBlock : resultDataSet.getDataBlocksList()) {

                            final List<String> queryPvNames = queryResultBlock.getPvNamesList();
                            final long queryBeginSeconds = queryResultBlock.getBeginTime().getEpochSeconds();
                            final long queryBeginNanos = queryResultBlock.getBeginTime().getNanoseconds();
                            final long queryEndSeconds = queryResultBlock.getEndTime().getEpochSeconds();
                            final long queryEndNanos = queryResultBlock.getEndTime().getNanoseconds();

                            final int numBucketsExpected = 2;

                            final QueryTestBase.QueryDataRequestParams params =
                                    new QueryTestBase.QueryDataRequestParams(
                                            queryPvNames,
                                            queryBeginSeconds,
                                            queryBeginNanos,
                                            queryEndSeconds,
                                            queryEndNanos
                                    );

                            final List<DataBucket> queryResultBuckets =
                                    queryServiceWrapper.queryDataStream(params, false, "");
                            assertEquals(numBucketsExpected, queryResultBuckets.size());
                            for (String pvName : queryPvNames) {
                                boolean foundPvBucket = false;
                                DataBucket matchingResponseBucket = null;
                                for (DataBucket responseBucket : queryResultBuckets) {
                                    if (Objects.equals(pvName, responseBucket.getDataValues().getDataColumn().getName())) {
                                        foundPvBucket = true;
                                        matchingResponseBucket = responseBucket;
                                        break;
                                    }
                                }
                                assertTrue(foundPvBucket);
                                final Timestamp matchingBucketTimestamp =
                                        matchingResponseBucket.getDataTimestamps().getSamplingClock().getStartTime();
                                assertEquals(queryBeginSeconds, matchingBucketTimestamp.getEpochSeconds());
                                assertEquals(queryBeginNanos, matchingBucketTimestamp.getNanoseconds());
                            }
                        }
                    }
                }
            }
        }

        // queryAnnotations() positive test for query by IdCriterion.
        {
            final AnnotationTestBase.QueryAnnotationsParams queryParams =
                    new AnnotationTestBase.QueryAnnotationsParams();
            queryParams.setIdCriterion(createAnnotationScenarioResult.annotationIdOwnerCraigmccComment1());

            final boolean expectReject = false;
            final String expectedRejectMessage ="";

            annotationServiceWrapper.sendAndVerifyQueryAnnotations(
                    queryParams,
                    expectReject,
                    expectedRejectMessage,
                    createAnnotationScenarioResult.expectedQueryByIdResultAnnotations());
        }

        // queryAnnotations() positive test for query by TextCriterion (over name field).
        {
            final String nameText = "first";
            final AnnotationTestBase.QueryAnnotationsParams queryParams =
                    new AnnotationTestBase.QueryAnnotationsParams();
            queryParams.setTextCriterion(nameText);

            final boolean expectReject = false;
            final String expectedRejectMessage ="";

            annotationServiceWrapper.sendAndVerifyQueryAnnotations(
                    queryParams,
                    expectReject,
                    expectedRejectMessage,
                    createAnnotationScenarioResult.expectedQueryByNameAnnotations());
        }

        // queryAnnotations() positive test for query by TextCriterion (over eventMetadata.description field).
        {
            final String eventDescriptionText = "1234";
            final AnnotationTestBase.QueryAnnotationsParams queryParams =
                    new AnnotationTestBase.QueryAnnotationsParams();
            queryParams.setTextCriterion(eventDescriptionText);

            final boolean expectReject = false;
            final String expectedRejectMessage ="";

            annotationServiceWrapper.sendAndVerifyQueryAnnotations(
                    queryParams,
                    expectReject,
                    expectedRejectMessage,
                    List.of(createAnnotationScenarioResult.annotationWithAllFieldsParams()));
        }

        // queryAnnotations() positive test for query by AnnotationCriterion (by id of related annotation).
        {
            final String relatedAnnotationId = createAnnotationScenarioResult.secondHalfAnnotationIds().get(0);
            final AnnotationTestBase.QueryAnnotationsParams queryParams =
                    new AnnotationTestBase.QueryAnnotationsParams();
            queryParams.setAnnotationsCriterion(relatedAnnotationId);

            final boolean expectReject = false;
            final String expectedRejectMessage ="";

            annotationServiceWrapper.sendAndVerifyQueryAnnotations(
                    queryParams,
                    expectReject,
                    expectedRejectMessage,
                    List.of(createAnnotationScenarioResult.annotationWithAllFieldsParams()));
        }

        // queryAnnotations() positive test for query by Tags (tag value).
        {
            final String tagValue = "beam loss";
            final AnnotationTestBase.QueryAnnotationsParams queryParams =
                    new AnnotationTestBase.QueryAnnotationsParams();
            queryParams.setTagsCriterion(tagValue);

            final boolean expectReject = false;
            final String expectedRejectMessage ="";

            annotationServiceWrapper.sendAndVerifyQueryAnnotations(
                    queryParams,
                    expectReject,
                    expectedRejectMessage,
                    List.of(createAnnotationScenarioResult.annotationWithAllFieldsParams()));
        }

        // queryAnnotations() positive test for query by Attributes (attribute key and value).
        {
            final String attributeKey = "sector";
            final String attributeValue = "01";
            final AnnotationTestBase.QueryAnnotationsParams queryParams =
                    new AnnotationTestBase.QueryAnnotationsParams();
            queryParams.setAttributesCriterion(attributeKey, attributeValue);

            final boolean expectReject = false;
            final String expectedRejectMessage ="";

            List<QueryAnnotationsResponse.AnnotationsResult.Annotation> matchingAnnotations =
                    annotationServiceWrapper.sendAndVerifyQueryAnnotations(
                            queryParams,
                            expectReject,
                            expectedRejectMessage,
                            List.of(createAnnotationScenarioResult.annotationWithAllFieldsParams()));

            // positive test for updating an annotation received in the query result
            final QueryAnnotationsResponse.AnnotationsResult.Annotation annotation = matchingAnnotations.get(0);
            final AnnotationTestBase.SaveAnnotationRequestParams createParams =
                    createAnnotationScenarioResult.annotationWithAllFieldsParams();
            final AnnotationTestBase.SaveAnnotationRequestParams updateParams =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            annotation.getId(),
                            createParams.ownerId,
                            createParams.name,
                            createParams.dataSetIds,
                            createParams.annotationIds,
                            "updated annotation",
                            createParams.tags,
                            createParams.attributeMap,
                            createParams.eventMetadataParams,
                            createParams.calculations
                    );
            annotationServiceWrapper.sendAndVerifySaveAnnotation(
                    updateParams, true, false, "");
        }

    }
}
