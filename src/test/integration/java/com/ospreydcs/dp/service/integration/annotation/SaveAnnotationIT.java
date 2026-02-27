package com.ospreydcs.dp.service.integration.annotation;

import com.ospreydcs.dp.service.annotation.AnnotationTestBase;
import org.junit.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SaveAnnotationIT extends AnnotationIntegrationTestIntermediate {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void testSaveAnnotationReject() {

        {
            // saveAnnotatio() negative test - request should be rejected because ownerId is not specified.

            final String unspecifiedOwnerId = "";
            final String dataSetId = "abcd1234";
            final String name = "negative test";
            AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(unspecifiedOwnerId, name, List.of(dataSetId));
            final String expectedRejectMessage = "SaveAnnotationRequest.ownerId must be specified";
            annotationServiceWrapper.sendAndVerifySaveAnnotation(
                    params, false, true, expectedRejectMessage);
        }

        {
            // saveAnnotatio() negative test - request should be rejected because name is not specified.

            final String ownerId = "craigmcc";
            final String dataSetId = "abcd1234";
            final String unspecifiedName = "";
            AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(ownerId, unspecifiedName, List.of(dataSetId));
            final String expectedRejectMessage = "SaveAnnotationRequest.name must be specified";
            annotationServiceWrapper.sendAndVerifySaveAnnotation(
                    params, false, true, expectedRejectMessage);
        }

        {
            // saveAnnotatio() negative test - request should be rejected because list of dataset ids is empty.

            final String ownerId = "craigmcc";
            final String emptyDataSetId = "";
            final String name = "negative test";
            AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(ownerId, name, new ArrayList<>());
            final String expectedRejectMessage = "SaveAnnotationRequest.dataSetIds must not be empty";
            annotationServiceWrapper.sendAndVerifySaveAnnotation(
                    params, false, true, expectedRejectMessage);
        }

        {
            // saveAnnotatio() negative test - request should be rejected because specified dataset doesn't exist

            final String ownerId = "craigmcc";
            final String invalidDataSetId = "junk12345";
            final String name = "negative test";
            AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(ownerId, name, List.of(invalidDataSetId));
            final String expectedRejectMessage = "no DataSetDocument found with id";
            annotationServiceWrapper.sendAndVerifySaveAnnotation(
                    params, false, true, expectedRejectMessage);
        }

    }

    @Test
    public void testSaveAnnotatioPositive() {

        final long startSeconds = Instant.now().getEpochSecond();
        final long startNanos = 0L;

        // ingest some data
        annotationIngestionScenario(startSeconds);

        // create some datasets
        CreateDataSetScenarioResult createDataSetScenarioResult = createDataSetScenario(startSeconds);

        // positive test case defined in superclass so it can be used to generate annotations for query and export tests
        CreateAnnotationScenarioResult createAnnotationScenarioResult = createAnnotationScenario(
                startSeconds,
                createDataSetScenarioResult.firstHalfDataSetId(),
                createDataSetScenarioResult.secondHalfDataSetId());

        {
            // saveAnnotatio() negative test - request includes an invalid associated annotation id

            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId());
            final String name = "negative test";
            final List<String> annotationIds = List.of("junk12345");
            final String comment = "This negative test case covers an annotation that specifies an invalid associated annotation id.";
            final List<String> tags = List.of("beam loss", "outage");
            final Map<String, String> attributeMap = Map.of("sector", "01", "subsystem", "vacuum");

            AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            null, ownerId,
                            name,
                            dataSetIds,
                            annotationIds,
                            comment,
                            tags,
                            attributeMap,
                            null);

            final boolean expectReject = true;
            final String expectedRejectMessage = "no AnnotationDocument found with id: junk12345";
            annotationServiceWrapper.sendAndVerifySaveAnnotation(
                    params, false, expectReject, expectedRejectMessage);
        }

    }

}
