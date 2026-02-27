package com.ospreydcs.dp.service.integration.annotation;

import com.ospreydcs.dp.grpc.v1.annotation.Calculations;
import com.ospreydcs.dp.grpc.v1.annotation.ExportDataRequest;
import com.ospreydcs.dp.grpc.v1.annotation.ExportDataResponse;
import com.ospreydcs.dp.grpc.v1.annotation.QueryAnnotationsResponse;
import com.ospreydcs.dp.grpc.v1.common.CalculationsSpec;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataTimestamps;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.service.annotation.AnnotationTestBase;
import com.ospreydcs.dp.service.common.protobuf.DataColumnUtility;
import com.ospreydcs.dp.service.common.protobuf.DataTimestampsUtility;
import com.ospreydcs.dp.service.common.protobuf.TimestampUtility;
import com.ospreydcs.dp.service.integration.ingest.GrpcIntegrationIngestionServiceWrapper;
import org.junit.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class AnnotationCalculationsIT extends AnnotationIntegrationTestIntermediate {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void testAnnotationCalculations() {

        final long startSeconds = Instant.now().getEpochSecond();
        final long startNanos = 0L;

        // ingest some data
        Map<String, GrpcIntegrationIngestionServiceWrapper.IngestionStreamInfo> validationMap =
                annotationIngestionScenario(startSeconds);

        // create some datasets
        CreateDataSetScenarioResult createDataSetScenarioResult = createDataSetScenario(startSeconds);

        // createAnnotation() with calculations negative test -
        // request should be rejected because: list of data frames is empty
        {
            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId());
            final String name = "negative test";

            // create calculations for request, with 2 data frames, each with 2 columns
            final Calculations.Builder calculationsBuilder = Calculations.newBuilder();
            final Calculations calculations = calculationsBuilder.build();

            final AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            null, ownerId,
                            name,
                            dataSetIds,
                            null,
                            null,
                            null,
                            null,
                            calculations);

            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "SaveAnnotationRequest.calculations.calculationDataFrames must not be empty";
            annotationServiceWrapper.sendAndVerifySaveAnnotation(params, false, expectReject, expectedRejectMessage);
        }

        // createAnnotation() with calculations negative test -
        // request should be rejected because: data frame name not specified
        {
            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId());
            final String name = "positive test: SamplingClock";

            // create calculations for request, with 2 data frames, each with 2 columns
            final Calculations.Builder calculationsBuilder = Calculations.newBuilder();
            for (int i = 0; i < 2; i++) {

                // create sampling clock
                final DataTimestamps dataTimestamps =
                        DataTimestampsUtility.dataTimestampsWithSamplingClock(
                                startSeconds + i, 500_000_000L, 250_000_000L, 2);

                // create data columns
                final List<DataColumn> dataColumns = new ArrayList<>();
                for (int j = 0; j < 2; j++) {
                    final String columnName = "calc-" + i + "-" + j;
                    final DataColumn dataColumn =
                            DataColumnUtility.dataColumnWithDoubleValues(columnName, List.of(0.0, 1.1));
                    dataColumns.add(dataColumn);
                }

                // create data frame
                final Calculations.CalculationsDataFrame dataFrame = Calculations.CalculationsDataFrame.newBuilder()
                        .setDataTimestamps(dataTimestamps)
                        .addAllDataColumns(dataColumns)
                        .build();
                calculationsBuilder.addCalculationDataFrames(dataFrame);
            }
            final Calculations calculations = calculationsBuilder.build();

            final AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            null, ownerId,
                            name,
                            dataSetIds,
                            null,
                            null,
                            null,
                            null,
                            calculations);

            final boolean expectReject = true;
            final String expectedRejectMessage = "CalculationDataFrame.name must be specified";
            annotationServiceWrapper.sendAndVerifySaveAnnotation(params, false, expectReject, expectedRejectMessage);
        }

        // createAnnotation() with calculations negative test -
        // request should be rejected because: DataTimestamps doesn't include SamplingClock or TimestampList
        {
            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId());
            final String name = "negative test";

            // create calculations for request, with 2 data frames, each with 2 columns
            final Calculations.Builder calculationsBuilder = Calculations.newBuilder();
            for (int i = 0; i < 2; i++) {

                // create data timestamps
                final DataTimestamps invalidDatatimestamps = DataTimestamps.newBuilder().build();

                // create data columns
                final List<DataColumn> dataColumns = new ArrayList<>();
                for (int j = 0; j < 2; j++) {
                    final String columnName = "calc-" + i + "-" + j;
                    final DataColumn dataColumn =
                            DataColumnUtility.dataColumnWithDoubleValues(columnName, List.of(0.0, 1.1));
                    dataColumns.add(dataColumn);
                }

                // create data frame
                final Calculations.CalculationsDataFrame dataFrame = Calculations.CalculationsDataFrame.newBuilder()
                        .setName("frame-" + i)
                        .setDataTimestamps(invalidDatatimestamps)
                        .addAllDataColumns(dataColumns)
                        .build();
                calculationsBuilder.addCalculationDataFrames(dataFrame);
            }
            final Calculations calculations = calculationsBuilder.build();

            final AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            null, ownerId,
                            name,
                            dataSetIds,
                            null,
                            null,
                            null,
                            null,
                            calculations);

            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "CalculationDataFrame.dataTimestamps must contain either SamplingClock or TimestampList";
            annotationServiceWrapper.sendAndVerifySaveAnnotation(params, false, expectReject, expectedRejectMessage);
        }

        // createAnnotation() with calculations negative test -
        // request should be rejected because: DataTimestamps doesn't include SamplingClock or TimestampList
        {
            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId());
            final String name = "negative test";

            // create calculations for request, with 2 data frames, each with 2 columns
            final Calculations.Builder calculationsBuilder = Calculations.newBuilder();
            for (int i = 0; i < 2; i++) {

                // create data timestamps
                final DataTimestamps invalidDatatimestamps = DataTimestamps.newBuilder().build();

                // create data columns
                final List<DataColumn> dataColumns = new ArrayList<>();
                for (int j = 0; j < 2; j++) {
                    final String columnName = "calc-" + i + "-" + j;
                    final DataColumn dataColumn =
                            DataColumnUtility.dataColumnWithDoubleValues(columnName, List.of(0.0, 1.1));
                    dataColumns.add(dataColumn);
                }

                // create data frame
                final Calculations.CalculationsDataFrame dataFrame = Calculations.CalculationsDataFrame.newBuilder()
                        .setName("frame-" + i)
                        .setDataTimestamps(invalidDatatimestamps)
                        .addAllDataColumns(dataColumns)
                        .build();
                calculationsBuilder.addCalculationDataFrames(dataFrame);
            }
            final Calculations calculations = calculationsBuilder.build();

            final AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            null, ownerId,
                            name,
                            dataSetIds,
                            null,
                            null,
                            null,
                            null,
                            calculations);

            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "CalculationDataFrame.dataTimestamps must contain either SamplingClock or TimestampList";
            annotationServiceWrapper.sendAndVerifySaveAnnotation(params, false, expectReject, expectedRejectMessage);
        }

        // createAnnotation() with calculations negative test -
        // request should be rejected because: DataColumns list is empty
        {
            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId());
            final String name = "negative test";

            // create calculations for request, with 2 data frames, each with 2 columns
            final Calculations.Builder calculationsBuilder = Calculations.newBuilder();
            for (int i = 0; i < 2; i++) {

                // create sampling clock
                final DataTimestamps dataTimestamps =
                        DataTimestampsUtility.dataTimestampsWithSamplingClock(
                                startSeconds + i, 500_000_000L, 250_000_000L, 2);

                // create data columns
                final List<DataColumn> emptyDataColumns = new ArrayList<>();

                // create data frame
                final Calculations.CalculationsDataFrame dataFrame = Calculations.CalculationsDataFrame.newBuilder()
                        .setName("frame-" + i)
                        .setDataTimestamps(dataTimestamps)
                        .addAllDataColumns(emptyDataColumns)
                        .build();
                calculationsBuilder.addCalculationDataFrames(dataFrame);
            }
            final Calculations calculations = calculationsBuilder.build();

            final AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            null, ownerId,
                            name,
                            dataSetIds,
                            null,
                            null,
                            null,
                            null,
                            calculations);

            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "CalculationDataFrame.dataColumns must not be empty";
            annotationServiceWrapper.sendAndVerifySaveAnnotation(params, false, expectReject, expectedRejectMessage);
        }

        // createAnnotation() with calculations negative test: rejected because startTime is invalid
        {
            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId());
            final String name = "negative test";

            // create calculations for request, with 2 data frames, each with 2 columns
            final Calculations.Builder calculationsBuilder = Calculations.newBuilder();
            for (int i = 0; i < 2; i++) {

                // create sampling clock
                final long invalidStartSeconds = 0L;
                final DataTimestamps dataTimestamps =
                        DataTimestampsUtility.dataTimestampsWithSamplingClock(
                                invalidStartSeconds, 500_000_000L, 250_000_000L, 2);

                // create data columns
                final List<DataColumn> dataColumns = new ArrayList<>();
                for (int j = 0; j < 2; j++) {
                    final String columnName = "calc-" + i + "-" + j;
                    final DataColumn dataColumn =
                            DataColumnUtility.dataColumnWithDoubleValues(columnName, List.of(0.0, 1.1));
                    dataColumns.add(dataColumn);
                }

                // create data frame
                final Calculations.CalculationsDataFrame dataFrame = Calculations.CalculationsDataFrame.newBuilder()
                        .setName("frame-" + i)
                        .setDataTimestamps(dataTimestamps)
                        .addAllDataColumns(dataColumns)
                        .build();
                calculationsBuilder.addCalculationDataFrames(dataFrame);
            }
            final Calculations calculations = calculationsBuilder.build();

            final AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            null, ownerId,
                            name,
                            dataSetIds,
                            null,
                            null,
                            null,
                            null,
                            calculations);

            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "CalculationDataFrame.dataTimestamps.samplingClock must specify startTime, periodNanos, and count";
            annotationServiceWrapper.sendAndVerifySaveAnnotation(params, false, expectReject, expectedRejectMessage);
        }

        // createAnnotation() with calculations negative test: rejected because periodNanos is invalid
        {
            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId());
            final String name = "negative test";

            // create calculations for request, with 2 data frames, each with 2 columns
            final Calculations.Builder calculationsBuilder = Calculations.newBuilder();
            for (int i = 0; i < 2; i++) {

                // create sampling clock
                final long invalidPeriodNanos = 0L;
                final DataTimestamps dataTimestamps =
                        DataTimestampsUtility.dataTimestampsWithSamplingClock(
                                startSeconds + i, 500_000_000L, invalidPeriodNanos, 2);

                // create data columns
                final List<DataColumn> dataColumns = new ArrayList<>();
                for (int j = 0; j < 2; j++) {
                    final String columnName = "calc-" + i + "-" + j;
                    final DataColumn dataColumn =
                            DataColumnUtility.dataColumnWithDoubleValues(columnName, List.of(0.0, 1.1));
                    dataColumns.add(dataColumn);
                }

                // create data frame
                final Calculations.CalculationsDataFrame dataFrame = Calculations.CalculationsDataFrame.newBuilder()
                        .setName("frame-" + i)
                        .setDataTimestamps(dataTimestamps)
                        .addAllDataColumns(dataColumns)
                        .build();
                calculationsBuilder.addCalculationDataFrames(dataFrame);
            }
            final Calculations calculations = calculationsBuilder.build();

            final AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            null, ownerId,
                            name,
                            dataSetIds,
                            null,
                            null,
                            null,
                            null,
                            calculations);

            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "CalculationDataFrame.dataTimestamps.samplingClock must specify startTime, periodNanos, and count";
            annotationServiceWrapper.sendAndVerifySaveAnnotation(params, false, expectReject, expectedRejectMessage);
        }

        // createAnnotation() with calculations negative test: rejected because count is invalid
        {
            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId());
            final String name = "negative test";

            // create calculations for request, with 2 data frames, each with 2 columns
            final Calculations.Builder calculationsBuilder = Calculations.newBuilder();
            for (int i = 0; i < 2; i++) {

                // create sampling clock
                final int invalidCount = 0;
                final DataTimestamps dataTimestamps =
                        DataTimestampsUtility.dataTimestampsWithSamplingClock(
                                startSeconds + i, 500_000_000L, 250_000_000L, invalidCount);

                // create data columns
                final List<DataColumn> dataColumns = new ArrayList<>();
                for (int j = 0; j < 2; j++) {
                    final String columnName = "calc-" + i + "-" + j;
                    final DataColumn dataColumn =
                            DataColumnUtility.dataColumnWithDoubleValues(columnName, List.of(0.0, 1.1));
                    dataColumns.add(dataColumn);
                }

                // create data frame
                final Calculations.CalculationsDataFrame dataFrame = Calculations.CalculationsDataFrame.newBuilder()
                        .setName("frame-" + i)
                        .setDataTimestamps(dataTimestamps)
                        .addAllDataColumns(dataColumns)
                        .build();
                calculationsBuilder.addCalculationDataFrames(dataFrame);
            }
            final Calculations calculations = calculationsBuilder.build();

            final AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            null, ownerId,
                            name,
                            dataSetIds,
                            null,
                            null,
                            null,
                            null,
                            calculations);

            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "CalculationDataFrame.dataTimestamps.samplingClock must specify startTime, periodNanos, and count";
            annotationServiceWrapper.sendAndVerifySaveAnnotation(params, false, expectReject, expectedRejectMessage);
        }

        // createAnnotation() with calculations negative test: rejected because DataColumn name not specified
        {
            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId());
            final String name = "negative test";

            // create calculations for request, with 2 data frames, each with 2 columns
            final Calculations.Builder calculationsBuilder = Calculations.newBuilder();
            for (int i = 0; i < 2; i++) {

                // create sampling clock
                final int count = 2;
                final DataTimestamps dataTimestamps =
                        DataTimestampsUtility.dataTimestampsWithSamplingClock(
                                startSeconds + i, 500_000_000L, 250_000_000L, count);

                // create data columns
                final List<DataColumn> dataColumns = new ArrayList<>();
                for (int j = 0; j < 2; j++) {
                    final String unspecifiedName = "";
                    final DataColumn dataColumn =
                            DataColumnUtility.dataColumnWithDoubleValues(unspecifiedName, List.of(0.0, 1.1));
                    dataColumns.add(dataColumn);
                }

                // create data frame
                final Calculations.CalculationsDataFrame dataFrame = Calculations.CalculationsDataFrame.newBuilder()
                        .setName("frame-" + i)
                        .setDataTimestamps(dataTimestamps)
                        .addAllDataColumns(dataColumns)
                        .build();
                calculationsBuilder.addCalculationDataFrames(dataFrame);
            }
            final Calculations calculations = calculationsBuilder.build();

            final AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            null, ownerId,
                            name,
                            dataSetIds,
                            null,
                            null,
                            null,
                            null,
                            calculations);

            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "CalculationDataFrame.dataColumns name must be specified for each DataColumn";
            annotationServiceWrapper.sendAndVerifySaveAnnotation(params, false, expectReject, expectedRejectMessage);
        }

        // createAnnotation() with calculations negative test: rejected because DataColumn is empty
        {
            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId());
            final String name = "negative test";

            // create calculations for request, with 2 data frames, each with 2 columns
            final Calculations.Builder calculationsBuilder = Calculations.newBuilder();
            for (int i = 0; i < 2; i++) {

                // create sampling clock
                final int count = 2;
                final DataTimestamps dataTimestamps =
                        DataTimestampsUtility.dataTimestampsWithSamplingClock(
                                startSeconds + i, 500_000_000L, 250_000_000L, count);

                // create data columns
                final List<DataColumn> dataColumns = new ArrayList<>();
                for (int j = 0; j < 2; j++) {
                    final String columnName = "calc-" + i + "-" + j;
                    final DataColumn emptyColumn =
                            DataColumnUtility.dataColumnWithDoubleValues(columnName, new ArrayList<>());
                    dataColumns.add(emptyColumn);
                }

                // create data frame
                final Calculations.CalculationsDataFrame dataFrame = Calculations.CalculationsDataFrame.newBuilder()
                        .setName("frame-" + i)
                        .setDataTimestamps(dataTimestamps)
                        .addAllDataColumns(dataColumns)
                        .build();
                calculationsBuilder.addCalculationDataFrames(dataFrame);
            }
            final Calculations calculations = calculationsBuilder.build();

            final AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            null, ownerId,
                            name,
                            dataSetIds,
                            null,
                            null,
                            null,
                            null,
                            calculations);

            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "CalculationDataFrame.dataColumns contains a DataColumn with no values";
            annotationServiceWrapper.sendAndVerifySaveAnnotation(params, false, expectReject, expectedRejectMessage);
        }

        // createAnnotation() with calculations positive test using DataTimestamps.SamplingClock.
        // Also provides positive and negative coverage for exporting calculations.
        {
            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId());
            final String name = "positive test: SamplingClock";

            // Create calculations for request, with 6 data frames, each with 2 columns.
            // Each data frame includes data values for one second of data.
            final Calculations.Builder calculationsBuilder = Calculations.newBuilder();
            for (int i = 0; i < 6; i++) {

                // create sampling clock
                // specifying 10 values per second (in the upper half second, every 20th of a second)
                final DataTimestamps dataTimestamps =
                        DataTimestampsUtility.dataTimestampsWithSamplingClock(
                                startSeconds + i, 500_000_000L, 50_000_000L, 10);

                // create data columns, each with 10 values
                final List<DataColumn> dataColumns = new ArrayList<>();
                for (int j = 0; j < 2; j++) {
                    final String columnName = "calc-" + i + "-" + j;
                    final DataColumn dataColumn =
                            DataColumnUtility.dataColumnWithDoubleValues(
                                    columnName,
                                    List.of(.50, .55, .60, .65, .70, .75, .80, .85, .90, .95));
                    dataColumns.add(dataColumn);
                }

                // create data frame
                final Calculations.CalculationsDataFrame dataFrame = Calculations.CalculationsDataFrame.newBuilder()
                        .setName("frame-" + i)
                        .setDataTimestamps(dataTimestamps)
                        .addAllDataColumns(dataColumns)
                        .build();
                calculationsBuilder.addCalculationDataFrames(dataFrame);
            }
            final Calculations calculations = calculationsBuilder.build();

            final AnnotationTestBase.SaveAnnotationRequestParams createAnnotationRequestParams =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            null, ownerId,
                            name,
                            dataSetIds,
                            null,
                            null,
                            null,
                            null,
                            calculations);

            final boolean expectReject = false;
            final String expectedRejectMessage = "";
            annotationServiceWrapper.sendAndVerifySaveAnnotation(createAnnotationRequestParams, false, expectReject, expectedRejectMessage);

            // queryAnnotations() positive test to verify calculations in query result annotation.
            // Result includes calculations id for annotation created above.
            String calculationsId;
            {
                final String nameText = "SamplingClock";
                final AnnotationTestBase.QueryAnnotationsParams queryAnnotationsParams =
                        new AnnotationTestBase.QueryAnnotationsParams();
                queryAnnotationsParams.setTextCriterion(nameText);

                List<QueryAnnotationsResponse.AnnotationsResult.Annotation> queryResultAnnotations =
                        annotationServiceWrapper.sendAndVerifyQueryAnnotations(
                                queryAnnotationsParams,
                                expectReject,
                                expectedRejectMessage,
                                List.of(createAnnotationRequestParams));
                assertEquals(1, queryResultAnnotations.size());
                calculationsId = queryResultAnnotations.get(0).getCalculations().getId();
            }

            // positive export test: export of dataset with calculations to csv.
            {
                // create CalculationsSpec with calculations id from query result
                CalculationsSpec calculationsSpec = CalculationsSpec.newBuilder()
                        .setCalculationsId(calculationsId)
                        .build();

                ExportDataResponse.ExportDataResult exportResult =
                        annotationServiceWrapper.sendAndVerifyExportData(
                                createDataSetScenarioResult.secondHalfDataSetId(),
                                createDataSetScenarioResult.secondHalfDataSetParams(),
                                calculationsSpec,
                                calculations,
                                ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_CSV,
                                10, // expect 10 buckets (2 pvs, 5 seconds, 1 bucket per second)
                                50, // 10 rows per second * 5 seconds (5 rows pv and calculations, 5 rows calculations)
                                validationMap,
                                false,
                                "");
            }

            // positive export test: export of only calculations (without dataset) to csv.
            {
                // create CalculationsSpec with calculations id from query result
                CalculationsSpec calculationsSpec = CalculationsSpec.newBuilder()
                        .setCalculationsId(calculationsId)
                        .build();

                ExportDataResponse.ExportDataResult exportResult =
                        annotationServiceWrapper.sendAndVerifyExportData(
                                null,
                                null,
                                calculationsSpec,
                                calculations,
                                ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_CSV,
                                0,
                                60, // 10 rows per second, 6 seconds
                                null,
                                false,
                                "");
            }

            // positive export test: export to csv filtering calculations columns using CalculationsSpec column map.
            {

                // create frame column map for filtering
                final Map<String, CalculationsSpec.ColumnNameList> dataFrameColumnsMap = new HashMap<>();
                final String frame1Name = "frame-2";
                final List<String> frame1Columns = List.of("calc-2-0", "calc-2-1");
                final CalculationsSpec.ColumnNameList frame1ColumnNameList = CalculationsSpec.ColumnNameList.newBuilder()
                        .addAllColumnNames(frame1Columns)
                        .build();
                dataFrameColumnsMap.put(frame1Name, frame1ColumnNameList);
                final String frame2Name = "frame-3";
                final List<String> frame2Columns = List.of("calc-3-1");
                final CalculationsSpec.ColumnNameList frame2ColumnNameList = CalculationsSpec.ColumnNameList.newBuilder()
                        .addAllColumnNames(frame2Columns)
                        .build();
                dataFrameColumnsMap.put(frame2Name, frame2ColumnNameList);

                // create CalculationsSpec with calculations id from query result
                CalculationsSpec calculationsSpec = CalculationsSpec.newBuilder()
                        .setCalculationsId(calculationsId)
                        .putAllDataFrameColumns(dataFrameColumnsMap)
                        .build();

                ExportDataResponse.ExportDataResult exportResult =
                        annotationServiceWrapper.sendAndVerifyExportData(
                                null,
                                null,
                                calculationsSpec,
                                calculations,
                                ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_CSV,
                                0,
                                20, // 10 rows per second, 2 seconds
                                null,
                                false,
                                "");
            }

            // positive export test: export of dataset with calculations to xlsx.
            {
                // create CalculationsSpec with calculations id from query result
                CalculationsSpec calculationsSpec = CalculationsSpec.newBuilder()
                        .setCalculationsId(calculationsId)
                        .build();

                ExportDataResponse.ExportDataResult exportResult =
                        annotationServiceWrapper.sendAndVerifyExportData(
                                createDataSetScenarioResult.secondHalfDataSetId(),
                                createDataSetScenarioResult.secondHalfDataSetParams(),
                                calculationsSpec,
                                calculations,
                                ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_XLSX,
                                10, // expect 10 buckets (2 pvs, 5 seconds, 1 bucket per second)
                                50, // 10 rows per second * 5 seconds (5 rows pv and calculations, 5 rows calculations)
                                validationMap,
                                false,
                                "");
            }

            // positive export test: export of only calculations (without dataset) to xlsx.
            {
                // create CalculationsSpec with calculations id from query result
                CalculationsSpec calculationsSpec = CalculationsSpec.newBuilder()
                        .setCalculationsId(calculationsId)
                        .build();

                ExportDataResponse.ExportDataResult exportResult =
                        annotationServiceWrapper.sendAndVerifyExportData(
                                null,
                                null,
                                calculationsSpec,
                                calculations,
                                ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_XLSX,
                                0,
                                60, // 10 rows per second, 6 seconds
                                null,
                                false,
                                "");
            }

            // positive export test: export to xlsx filtering calculations columns using CalculationsSpec column map.
            {

                // create frame column map for filtering
                final Map<String, CalculationsSpec.ColumnNameList> dataFrameColumnsMap = new HashMap<>();
                final String frame1Name = "frame-2";
                final List<String> frame1Columns = List.of("calc-2-0", "calc-2-1");
                final CalculationsSpec.ColumnNameList frame1ColumnNameList = CalculationsSpec.ColumnNameList.newBuilder()
                        .addAllColumnNames(frame1Columns)
                        .build();
                dataFrameColumnsMap.put(frame1Name, frame1ColumnNameList);
                final String frame2Name = "frame-3";
                final List<String> frame2Columns = List.of("calc-3-1");
                final CalculationsSpec.ColumnNameList frame2ColumnNameList = CalculationsSpec.ColumnNameList.newBuilder()
                        .addAllColumnNames(frame2Columns)
                        .build();
                dataFrameColumnsMap.put(frame2Name, frame2ColumnNameList);

                // create CalculationsSpec with calculations id from query result
                CalculationsSpec calculationsSpec = CalculationsSpec.newBuilder()
                        .setCalculationsId(calculationsId)
                        .putAllDataFrameColumns(dataFrameColumnsMap)
                        .build();

                ExportDataResponse.ExportDataResult exportResult =
                        annotationServiceWrapper.sendAndVerifyExportData(
                                null,
                                null,
                                calculationsSpec,
                                calculations,
                                ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_XLSX,
                                0,
                                20, // 10 rows per second, 2 seconds
                                null,
                                false,
                                "");
            }

            // positive export test: export of dataset with calculations to hdf5.
            {
                // create CalculationsSpec with calculations id from query result
                CalculationsSpec calculationsSpec = CalculationsSpec.newBuilder()
                        .setCalculationsId(calculationsId)
                        .build();

                ExportDataResponse.ExportDataResult exportResult =
                        annotationServiceWrapper.sendAndVerifyExportData(
                                createDataSetScenarioResult.secondHalfDataSetId(),
                                createDataSetScenarioResult.secondHalfDataSetParams(),
                                calculationsSpec,
                                calculations,
                                ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_HDF5,
                                10, // expect 10 buckets (2 pvs, 5 seconds, 1 bucket per second)
                                0,
                                validationMap,
                                false,
                                "");
            }

            // positive export test: export of only calculations (without dataset) to hdf5.
            {
                // create CalculationsSpec with calculations id from query result
                CalculationsSpec calculationsSpec = CalculationsSpec.newBuilder()
                        .setCalculationsId(calculationsId)
                        .build();

                ExportDataResponse.ExportDataResult exportResult =
                        annotationServiceWrapper.sendAndVerifyExportData(
                                null,
                                null,
                                calculationsSpec,
                                calculations,
                                ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_HDF5,
                                10,
                                0, // 10 rows per second, 6 seconds
                                null,
                                false,
                                "");
            }

            // positive export test: export to hdf5 filtering calculations columns using CalculationsSpec column map.
            {

                // create frame column map for filtering
                final Map<String, CalculationsSpec.ColumnNameList> dataFrameColumnsMap = new HashMap<>();
                final String frame1Name = "frame-2";
                final List<String> frame1Columns = List.of("calc-2-0", "calc-2-1");
                final CalculationsSpec.ColumnNameList frame1ColumnNameList = CalculationsSpec.ColumnNameList.newBuilder()
                        .addAllColumnNames(frame1Columns)
                        .build();
                dataFrameColumnsMap.put(frame1Name, frame1ColumnNameList);
                final String frame2Name = "frame-3";
                final List<String> frame2Columns = List.of("calc-3-1");
                final CalculationsSpec.ColumnNameList frame2ColumnNameList = CalculationsSpec.ColumnNameList.newBuilder()
                        .addAllColumnNames(frame2Columns)
                        .build();
                dataFrameColumnsMap.put(frame2Name, frame2ColumnNameList);

                // create CalculationsSpec with calculations id from query result
                CalculationsSpec calculationsSpec = CalculationsSpec.newBuilder()
                        .setCalculationsId(calculationsId)
                        .putAllDataFrameColumns(dataFrameColumnsMap)
                        .build();

                ExportDataResponse.ExportDataResult exportResult =
                        annotationServiceWrapper.sendAndVerifyExportData(
                                null,
                                null,
                                calculationsSpec,
                                calculations,
                                ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_HDF5,
                                10,
                                0,
                                null,
                                false,
                                "");
            }

            // negative export test: blank calculations id
            {
                // create CalculationsSpec with calculations id from query result
                CalculationsSpec calculationsSpec = CalculationsSpec.newBuilder()
                        .setCalculationsId("")
                        .build();

                ExportDataResponse.ExportDataResult exportResult =
                        annotationServiceWrapper.sendAndVerifyExportData(
                                createDataSetScenarioResult.secondHalfDataSetId(),
                                createDataSetScenarioResult.secondHalfDataSetParams(),
                                calculationsSpec,
                                null,
                                ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_CSV,
                                0,
                                0,
                                validationMap,
                                true,
                                "ExportDataRequest.calculationsSpec.calculationsId must be specified");
            }

            // negative export test: invalid calculations id
            {
                // create CalculationsSpec with calculations id from query result
                CalculationsSpec calculationsSpec = CalculationsSpec.newBuilder()
                        .setCalculationsId("abcde12345")
                        .build();

                ExportDataResponse.ExportDataResult exportResult =
                        annotationServiceWrapper.sendAndVerifyExportData(
                                createDataSetScenarioResult.secondHalfDataSetId(),
                                createDataSetScenarioResult.secondHalfDataSetParams(),
                                calculationsSpec,
                                null,
                                ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_CSV,
                                0,
                                0,
                                validationMap,
                                true,
                                "CalculationsDocument with id abcde12345 not found");
            }

            // negative export test: empty column name list in CalculationsSpec column map for
            // filtering calculations columns in export
            {

                // create frame column map for filtering
                final Map<String, CalculationsSpec.ColumnNameList> dataFrameColumnsMap = new HashMap<>();
                final String frame1Name = "frame-2";
                final List<String> frame1Columns = List.of();
                final CalculationsSpec.ColumnNameList frame1ColumnNameList = CalculationsSpec.ColumnNameList.newBuilder()
                        .addAllColumnNames(frame1Columns)
                        .build();
                dataFrameColumnsMap.put(frame1Name, frame1ColumnNameList);

                // create CalculationsSpec with calculations id from query result
                CalculationsSpec calculationsSpec = CalculationsSpec.newBuilder()
                        .setCalculationsId(calculationsId)
                        .putAllDataFrameColumns(dataFrameColumnsMap)
                        .build();

                ExportDataResponse.ExportDataResult exportResult =
                        annotationServiceWrapper.sendAndVerifyExportData(
                                null,
                                null,
                                calculationsSpec,
                                calculations,
                                ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_CSV,
                                0,
                                0, // 10 rows per second, 2 seconds
                                null,
                                true,
                                "ExportDataRequest.calculationsSpec.dataFrameColumns list must not be empty");
            }

            // negative export test: blank column name in CalculationsSpec column map for
            // filtering calculations columns in export
            {

                // create frame column map for filtering
                final Map<String, CalculationsSpec.ColumnNameList> dataFrameColumnsMap = new HashMap<>();
                final String frame1Name = "frame-2";
                final List<String> frame1Columns = List.of("");
                final CalculationsSpec.ColumnNameList frame1ColumnNameList = CalculationsSpec.ColumnNameList.newBuilder()
                        .addAllColumnNames(frame1Columns)
                        .build();
                dataFrameColumnsMap.put(frame1Name, frame1ColumnNameList);

                // create CalculationsSpec with calculations id from query result
                CalculationsSpec calculationsSpec = CalculationsSpec.newBuilder()
                        .setCalculationsId(calculationsId)
                        .putAllDataFrameColumns(dataFrameColumnsMap)
                        .build();

                ExportDataResponse.ExportDataResult exportResult =
                        annotationServiceWrapper.sendAndVerifyExportData(
                                null,
                                null,
                                calculationsSpec,
                                calculations,
                                ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_CSV,
                                0,
                                0, // 10 rows per second, 2 seconds
                                null,
                                true,
                                "ExportDataRequest.calculationsSpec.dataFrameColumns includes blank column name");
            }

            // negative export test: CalculationsSpec frameColumnNames map uses invalid frame name.
            {

                // create frame column map for filtering
                final Map<String, CalculationsSpec.ColumnNameList> dataFrameColumnsMap = new HashMap<>();
                final String frame1Name = "junk";
                final List<String> frame1Columns = List.of("calc-2-0", "calc-2-1");
                final CalculationsSpec.ColumnNameList frame1ColumnNameList = CalculationsSpec.ColumnNameList.newBuilder()
                        .addAllColumnNames(frame1Columns)
                        .build();
                dataFrameColumnsMap.put(frame1Name, frame1ColumnNameList);
                final String frame2Name = "frame-3";
                final List<String> frame2Columns = List.of("calc-3-1");
                final CalculationsSpec.ColumnNameList frame2ColumnNameList = CalculationsSpec.ColumnNameList.newBuilder()
                        .addAllColumnNames(frame2Columns)
                        .build();
                dataFrameColumnsMap.put(frame2Name, frame2ColumnNameList);

                // create CalculationsSpec with calculations id from query result
                CalculationsSpec calculationsSpec = CalculationsSpec.newBuilder()
                        .setCalculationsId(calculationsId)
                        .putAllDataFrameColumns(dataFrameColumnsMap)
                        .build();

                ExportDataResponse.ExportDataResult exportResult =
                        annotationServiceWrapper.sendAndVerifyExportData(
                                null,
                                null,
                                calculationsSpec,
                                calculations,
                                ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_CSV,
                                0,
                                20, // 10 rows per second, 2 seconds
                                null,
                                true,
                                "ExportDataRequest.CalculationsSpec.dataFrameColumns includes invalid frame name: junk");
            }

            // negative export test: CalculationsSpec frameColumnNames map uses invalid column name for frame.
            {

                // create frame column map for filtering
                final Map<String, CalculationsSpec.ColumnNameList> dataFrameColumnsMap = new HashMap<>();
                final String frame1Name = "frame-2";
                final List<String> frame1Columns = List.of("calc-2-0", "junk");
                final CalculationsSpec.ColumnNameList frame1ColumnNameList = CalculationsSpec.ColumnNameList.newBuilder()
                        .addAllColumnNames(frame1Columns)
                        .build();
                dataFrameColumnsMap.put(frame1Name, frame1ColumnNameList);
                final String frame2Name = "frame-3";
                final List<String> frame2Columns = List.of("calc-3-1");
                final CalculationsSpec.ColumnNameList frame2ColumnNameList = CalculationsSpec.ColumnNameList.newBuilder()
                        .addAllColumnNames(frame2Columns)
                        .build();
                dataFrameColumnsMap.put(frame2Name, frame2ColumnNameList);

                // create CalculationsSpec with calculations id from query result
                CalculationsSpec calculationsSpec = CalculationsSpec.newBuilder()
                        .setCalculationsId(calculationsId)
                        .putAllDataFrameColumns(dataFrameColumnsMap)
                        .build();

                ExportDataResponse.ExportDataResult exportResult =
                        annotationServiceWrapper.sendAndVerifyExportData(
                                null,
                                null,
                                calculationsSpec,
                                calculations,
                                ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_CSV,
                                0,
                                20, // 10 rows per second, 2 seconds
                                null,
                                true,
                                "ExportDataRequest.CalculationsSpec.dataFrameColumns includes invalid column name: junk for frame: frame-2");
            }

        }

        // createAnnotation() with calculations positive test where calculations time range doesn't overlap
        // dataset time range
        {
            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId());
            final String name = "calculations time range doesn't overlap dataset time range";

            // Create calculations for request, with 6 data frames, each with 2 columns.
            // Each data frame includes data values for one second of data.
            final Calculations.Builder calculationsBuilder = Calculations.newBuilder();
            for (int i = 0; i < 6; i++) {

                // create sampling clock
                // specifying 10 values per second (in the upper half second, every 20th of a second)
                final DataTimestamps dataTimestamps =
                        DataTimestampsUtility.dataTimestampsWithSamplingClock(
                                startSeconds + i+10,
                                500_000_000L,
                                50_000_000L,
                                10);

                // create data columns, each with 10 values
                final List<DataColumn> dataColumns = new ArrayList<>();
                for (int j = 0; j < 2; j++) {
                    final String columnName = "calc-" + i + "-" + j;
                    final DataColumn dataColumn =
                            DataColumnUtility.dataColumnWithDoubleValues(
                                    columnName,
                                    List.of(.50, .55, .60, .65, .70, .75, .80, .85, .90, .95));
                    dataColumns.add(dataColumn);
                }

                // create data frame
                final Calculations.CalculationsDataFrame dataFrame = Calculations.CalculationsDataFrame.newBuilder()
                        .setName("frame-" + i)
                        .setDataTimestamps(dataTimestamps)
                        .addAllDataColumns(dataColumns)
                        .build();
                calculationsBuilder.addCalculationDataFrames(dataFrame);
            }
            final Calculations calculations = calculationsBuilder.build();

            final AnnotationTestBase.SaveAnnotationRequestParams createAnnotationRequestParams =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            null, ownerId,
                            name,
                            dataSetIds,
                            null,
                            null,
                            null,
                            null,
                            calculations);

            final boolean expectReject = false;
            final String expectedRejectMessage = "";
            annotationServiceWrapper.sendAndVerifySaveAnnotation(createAnnotationRequestParams, false, expectReject, expectedRejectMessage);

            // queryAnnotations() positive test to verify calculations in query result annotation.
            // Result includes calculations id for annotation created above.
            String calculationsId;
            {
                final String nameText = "time range";
                final AnnotationTestBase.QueryAnnotationsParams queryAnnotationsParams =
                        new AnnotationTestBase.QueryAnnotationsParams();
                queryAnnotationsParams.setTextCriterion(nameText);

                List<QueryAnnotationsResponse.AnnotationsResult.Annotation> queryResultAnnotations =
                        annotationServiceWrapper.sendAndVerifyQueryAnnotations(
                                queryAnnotationsParams,
                                expectReject,
                                expectedRejectMessage,
                                List.of(createAnnotationRequestParams));
                assertEquals(1, queryResultAnnotations.size());
                calculationsId = queryResultAnnotations.get(0).getCalculations().getId();
            }

            // Positive export test: export where time range of calculations doesn't overlap time range of dataset.
            // Export output file contains data for the dataset with empty columns for the calculations.
            {
                // create CalculationsSpec with calculations id from query result
                CalculationsSpec calculationsSpec = CalculationsSpec.newBuilder()
                        .setCalculationsId(calculationsId)
                        .build();

                ExportDataResponse.ExportDataResult exportResult =
                        annotationServiceWrapper.sendAndVerifyExportData(
                                createDataSetScenarioResult.secondHalfDataSetId(),
                                createDataSetScenarioResult.secondHalfDataSetParams(),
                                calculationsSpec,
                                calculations,
                                ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_CSV,
                                10, // expect 10 buckets (2 pvs, 5 seconds, 1 bucket per second)
                                25, // 5 rows per second for specified dataset, no calculations rows
                                validationMap,
                                false,
                                "");

                System.err.println("dataset id for non-overlapping time range: " + createDataSetScenarioResult.secondHalfDataSetId());
            }

        }

        // createAnnotation() with calculations negative test using DataTimestamps.TimestampList
        // rejected because TimestampList is empty
        {
            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId());
            final String name = "negative test";

            // create calculations for request, with 2 data frames, each with 2 columns
            final Calculations.Builder calculationsBuilder = Calculations.newBuilder();
            for (int i = 0 ; i < 2 ; i++) {

                // create sampling clock with TimestampList
                final List<Timestamp> emptyTimestampList = new ArrayList<>();
                final DataTimestamps dataTimestamps =
                        DataTimestampsUtility.dataTimestampsWithTimestampList(emptyTimestampList);

                // create data columns
                final List<DataColumn> dataColumns = new ArrayList<>();
                for (int j = 0 ; j < 2 ; j++) {
                    final String columnName = "calc-" + i + "-" + j;
                    final DataColumn dataColumn =
                            DataColumnUtility.dataColumnWithDoubleValues(columnName, List.of(0.0, 1.1));
                    dataColumns.add(dataColumn);
                }

                // create data frame
                final Calculations.CalculationsDataFrame dataFrame = Calculations.CalculationsDataFrame.newBuilder()
                        .setName("frame-" + i)
                        .setDataTimestamps(dataTimestamps)
                        .addAllDataColumns(dataColumns)
                        .build();
                calculationsBuilder.addCalculationDataFrames(dataFrame);
            }
            final Calculations calculations = calculationsBuilder.build();

            final AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            null, ownerId,
                            name,
                            dataSetIds,
                            null,
                            null,
                            null,
                            null,
                            calculations);

            final boolean expectReject = true;
            final String expectedRejectMessage = "CalculationDataFrame.dataTimestamps.timestampList must not be empty";
            annotationServiceWrapper.sendAndVerifySaveAnnotation(params, false, expectReject, expectedRejectMessage);
        }

        // createAnnotation() with calculations positive test using DataTimestamps.TimestampList
        {
            final String ownerId = "craigmcc";
            final List<String> dataSetIds = List.of(createDataSetScenarioResult.secondHalfDataSetId());
            final String name = "positive test: TimestampList";

            // create calculations for request, with 2 data frames, each with 2 columns
            final Calculations.Builder calculationsBuilder = Calculations.newBuilder();
            for (int i = 0 ; i < 2 ; i++) {

                // create sampling clock with TimestampList
                final List<Timestamp> timestampList = new ArrayList<>();
                final Timestamp timestamp1 =
                        TimestampUtility.timestampFromSeconds(startSeconds+i, 500_000_000L);
                timestampList.add(timestamp1);
                final Timestamp timestamp2 =
                        TimestampUtility.timestampFromSeconds(startSeconds+i, 750_000_000L);
                timestampList.add(timestamp2);
                final DataTimestamps dataTimestamps =
                        DataTimestampsUtility.dataTimestampsWithTimestampList(timestampList);

                // create data columns
                final List<DataColumn> dataColumns = new ArrayList<>();
                for (int j = 0 ; j < 2 ; j++) {
                    final String columnName = "calc-" + i + "-" + j;
                    final DataColumn dataColumn =
                            DataColumnUtility.dataColumnWithDoubleValues(columnName, List.of(0.0, 1.1));
                    dataColumns.add(dataColumn);
                }

                // create data frame
                final Calculations.CalculationsDataFrame dataFrame = Calculations.CalculationsDataFrame.newBuilder()
                        .setName("frame-" + i)
                        .setDataTimestamps(dataTimestamps)
                        .addAllDataColumns(dataColumns)
                        .build();
                calculationsBuilder.addCalculationDataFrames(dataFrame);
            }
            final Calculations calculations = calculationsBuilder.build();

            final AnnotationTestBase.SaveAnnotationRequestParams params =
                    new AnnotationTestBase.SaveAnnotationRequestParams(
                            null, ownerId,
                            name,
                            dataSetIds,
                            null,
                            null,
                            null,
                            null,
                            calculations);

            final boolean expectReject = false;
            final String expectedRejectMessage = "";
            annotationServiceWrapper.sendAndVerifySaveAnnotation(params, false, expectReject, expectedRejectMessage);

            // queryAnnotations() positive test to verify calculations in query result annotation
            // uses calculation created above
            {
                final String nameText = "TimestampList";
                final AnnotationTestBase.QueryAnnotationsParams queryParams =
                        new AnnotationTestBase.QueryAnnotationsParams();
                queryParams.setTextCriterion(nameText);

                annotationServiceWrapper.sendAndVerifyQueryAnnotations(
                        queryParams,
                        expectReject,
                        expectedRejectMessage,
                        List.of(params));
            }
        }

    }

}
