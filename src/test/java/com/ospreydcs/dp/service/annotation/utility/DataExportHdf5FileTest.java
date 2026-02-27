package com.ospreydcs.dp.service.annotation.utility;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.service.annotation.AnnotationTestBase;
import com.ospreydcs.dp.service.annotation.handler.mongo.export.DataExportHdf5File;
import com.ospreydcs.dp.service.common.bson.column.DataColumnDocument;
import com.ospreydcs.dp.service.common.bson.DataTimestampsDocument;
import com.ospreydcs.dp.service.common.bson.TimestampDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.column.DoubleColumnDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataBlockDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.time.Instant;
import java.util.*;

import static org.junit.Assert.*;

/**
 * This class provides unit test coverage for the exportData() API with output to HDF5 format.  NOTE: the test writes
 * hdf5 files to /tmp on disk.
 */
public class DataExportHdf5FileTest {

    /**
     * This test case provides positive test coverage for exporting data to HDF5 format.  It creates and initializes
     * the DataExportHdf5File, which creates the file on disk and initializes the indexing structure.  It then creates
     * a dataset and a couple of BucketDocuments, writes them to the HDF5 file, and verifies the file content.
     */
    @Test
    public void testCreateExportFile() {

        // create export file and top-level group index structure
        final String exportFilePathString = "/tmp/testCreateExportFile.h5";
        DataExportHdf5File exportHdf5File = null;
        try {
            exportHdf5File = new DataExportHdf5File(exportFilePathString);
        } catch (DpException e) {
            fail("exception creating " + exportFilePathString);
        }
        Objects.requireNonNull(exportHdf5File);

        final Instant instantNow = Instant.now();
        final Instant firstInstant = instantNow.minusNanos(instantNow.getNano());
        final long firstSeconds = instantNow.getEpochSecond();
        final long firstNanos = 0;
        final long lastSeconds = firstSeconds;
        final long lastNanos = 900000000L;

        // create dataset for export
        final DataSetDocument dataset = new DataSetDocument();
        dataset.setName("export dataset");
        dataset.setDescription("test coverage for export to hdf5 file");
        dataset.setId(new ObjectId("1234abcd1234abcd1234abcd")); // must be 24 digit hex
        final List<DataBlockDocument> dataBlocks = new ArrayList<>();
        DataBlockDocument dataBlock1 = new DataBlockDocument();
        dataBlock1.setPvNames(List.of("S01-GCC01", "S01-GCC02"));
        final Timestamp samplingClockStartTime = Timestamp.newBuilder()
                .setEpochSeconds(firstSeconds)
                .setNanoseconds(firstNanos)
                .build();
        final Timestamp stopTimestamp = Timestamp.newBuilder()
                .setEpochSeconds(lastSeconds)
                .setNanoseconds(lastNanos)
                .build();
        dataBlock1.setBeginTime(TimestampDocument.fromTimestamp(samplingClockStartTime));
        dataBlock1.setEndTime(TimestampDocument.fromTimestamp(stopTimestamp));
        dataBlocks.add(dataBlock1);
        dataset.setDataBlocks(dataBlocks);

        final int sampleCount = 10;
        final long samplePeriod = 100000000L;
        final SamplingClock samplingClock =
                SamplingClock.newBuilder()
                        .setStartTime(samplingClockStartTime)
                        .setCount(sampleCount)
                        .setPeriodNanos(samplePeriod)
                        .build();
        final DataTimestamps dataTimestamps = DataTimestamps.newBuilder().setSamplingClock(samplingClock).build();
        final DataTimestampsDocument dataTimestampsDocument = DataTimestampsDocument.fromDataTimestamps(dataTimestamps);

        final String providerId = "S01 vacuum provider";

        // write dataset
        exportHdf5File.writeDataSet(dataset);

        // create first BucketDocument for S01-GCC01
        BucketDocument pv1BucketDocument = null;
        {
            pv1BucketDocument = new BucketDocument();
            pv1BucketDocument.setPvName("S01-GCC01");
            pv1BucketDocument.setDataTimestamps(dataTimestampsDocument);
            final DataColumn.Builder dataColumnBuilder = DataColumn.newBuilder();
            dataColumnBuilder.setName(pv1BucketDocument.getPvName());
            for (int i = 0; i < sampleCount; ++i) {
                DataValue dataValue = DataValue.newBuilder().setIntValue(i).build();
                dataColumnBuilder.addDataValues(dataValue);
            }
            final DataColumn dataColumn = dataColumnBuilder.build();
            final DataColumnDocument dataColumnDocument = DataColumnDocument.fromDataColumn(dataColumn);
            pv1BucketDocument.setDataColumn(dataColumnDocument);
            pv1BucketDocument.setProviderId(providerId);
            exportHdf5File.writeBucket(pv1BucketDocument);
        }

        // create second BucketDocument for S01-GCC02
        BucketDocument pv2BucketDocument = null;
        {
            pv2BucketDocument = new BucketDocument();
            pv2BucketDocument.setPvName("S01-GCC02");
            pv2BucketDocument.setDataTimestamps(dataTimestampsDocument);
            final DataColumn.Builder dataColumnBuilder = DataColumn.newBuilder();
            dataColumnBuilder.setName(pv2BucketDocument.getPvName());
            for (int i = sampleCount; i < 20; ++i) {
                DataValue dataValue = DataValue.newBuilder().setIntValue(i).build();
                dataColumnBuilder.addDataValues(dataValue);
            }
            DataColumn dataColumn = dataColumnBuilder.build();
            final DataColumnDocument dataColumnDocument = DataColumnDocument.fromDataColumn(dataColumn);
            pv2BucketDocument.setDataColumn(dataColumnDocument);
            pv2BucketDocument.setProviderId(providerId);
            exportHdf5File.writeBucket(pv2BucketDocument);
        }

        // create third BucketDocument containing a DoubleColumn to test handling the new API protobuf column data structures
        BucketDocument doubleBucketDocument = null;
        {
            doubleBucketDocument = new BucketDocument();
            doubleBucketDocument.setPvName("S01-GCC03");
            doubleBucketDocument.setDataTimestamps(dataTimestampsDocument);
            DoubleColumn.Builder doubleColumnBuilder = DoubleColumn.newBuilder();
            doubleColumnBuilder.setName("S01-GCC03");
            doubleColumnBuilder.addValues(12.34);
            doubleColumnBuilder.addValues(34.56);
            DoubleColumn doubleColumn = doubleColumnBuilder.build();
            final DoubleColumnDocument doubleColumnDocument = DoubleColumnDocument.fromDoubleColumn(doubleColumn);
            doubleBucketDocument.setDataColumn(doubleColumnDocument);
            doubleBucketDocument.setProviderId(providerId);
            doubleBucketDocument.setClientRequestId(UUID.randomUUID().toString());
            exportHdf5File.writeBucket(doubleBucketDocument);
        }

        exportHdf5File.close();

        // verify file content
        final IHDF5Reader reader = HDF5Factory.openForReading("/tmp/testCreateExportFile.h5");
        AnnotationTestBase.verifyDatasetHdf5Content(reader, dataset);
        AnnotationTestBase.verifyBucketDocumentHdf5Content(reader, pv1BucketDocument);
        AnnotationTestBase.verifyBucketDocumentHdf5Content(reader, pv2BucketDocument);
        AnnotationTestBase.verifyBucketDocumentHdf5Content(reader, doubleBucketDocument);

        // close reader
        reader.close();
    }
}
