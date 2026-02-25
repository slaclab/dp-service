package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.common.CalculationsSpec;
import com.ospreydcs.dp.service.annotation.handler.model.ExportConfiguration;
import com.ospreydcs.dp.service.annotation.handler.model.HandlerExportDataRequest;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.export.TabularDataExportFileInterface;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.calculations.CalculationsDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataBlockDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.model.TimestampDataMap;
import com.ospreydcs.dp.service.common.utility.TabularDataUtility;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This is the abstract intermediate base class for tabular data export formats (e.g., csv and xlsx).  This class
 * overrides the abstract method exportData_() to export the dataset and calculations specified in the request to the
 * tabular output driver.  Instead of defining abstract methods for subclasses, the TabularDataExportFileInterface
 * is used to specify the required interface for the concrete tabular output classes.  Derived classes override
 * createExportFile_ to initialize the instance implementing TabularDataExportFileInterface.
 */
public abstract class ExportDataJobAbstractTabular extends ExportDataJobBase {

    // constants
    public static final String COLUMN_HEADER_SECONDS = "seconds";
    public static final String COLUMN_HEADER_NANOS = "nanos";

    // instance variables
    private TabularDataExportFileInterface exportFile;

    public ExportDataJobAbstractTabular(
            HandlerExportDataRequest handlerRequest,
            MongoAnnotationClientInterface mongoAnnotationClient,
            MongoQueryClientInterface mongoQueryClient
    ) {
        super(handlerRequest, mongoAnnotationClient, mongoQueryClient);
    }

    protected abstract TabularDataExportFileInterface createExportFile_(
            DataSetDocument dataset, String serverFilePath) throws DpException;

    /**
     * Exports the supplied dataset and calculations documents to the tabular export output file. Calls abstract method
     * createExportFile_() to create the output file interface.  Executes mongo queries to retrieve the specified
     * dataset and calculations objects, using frameColumnNamesMap to filter the calculations columns, and creates a
     * tabular data structure from the result.  It then uses the output file interface object methods to write header rows,
     * data rows, and close the file.
     *
     * NOTE: this pulls all the data into memory at once, instead of handling
     * the data row by row or whatever. It could definitely be optimized, but it's not obvious how to build and write
     * a tabular data structure to file in a row-by-row fashion (since the number of rows isn't known until all the
     * timestamps of all the columns are known, since they are all on different timescales).  We would need to know
     * the timestamp period at the outset, or something along those lines.
     *
     * @param datasetDocument
     * @param calculationsDocument
     * @param frameColumnNamesMap
     * @param serverFilePath
     * @return
     */
    @Override
    protected ExportDataStatus exportData_(
            DataSetDocument datasetDocument,
            CalculationsDocument calculationsDocument,
            Map<String, CalculationsSpec.ColumnNameList> frameColumnNamesMap,
            String serverFilePath
    ) {
        // create file for export
        try {
            exportFile = createExportFile_(datasetDocument, serverFilePath);
        } catch (DpException e) {
            final String errorMsg = "exception opening export file " + serverFilePath + ": " + e.getMessage();
            logger.error("id: {}, error: {}", this.handlerRequest.responseObserver.hashCode(), errorMsg);
            return new ExportDataStatus(true, errorMsg);
        }

        // create temporary tabular data structure for writing to file
        final TimestampDataMap tableValueMap = new TimestampDataMap();
        int tableDataSize = 0;

        // add data for each data block in dataset to tabular data structure
        Instant exportBeginInstant = null;
        Instant exportEndInstant = null;
        boolean first = true;
        if (datasetDocument != null) {
            for (DataBlockDocument dataBlock : datasetDocument.getDataBlocks()) {

                final MongoCursor<BucketDocument> cursor =
                        this.mongoQueryClient.executeDataBlockQuery(dataBlock);

                if (cursor == null) {
                    final String errorMsg = "unknown error executing data block query for export file: " + serverFilePath;
                    logger.error("id: {}, error: {}", this.handlerRequest.responseObserver.hashCode(), errorMsg);
                    return new ExportDataStatus(true, errorMsg);
                }

                if (!cursor.hasNext()) {
                    final String errorMsg = "data block query returned no data";
                    logger.trace("id: {}, error: {}", this.handlerRequest.responseObserver.hashCode(), errorMsg);
                    return new ExportDataStatus(true, errorMsg);
                }

                // get time range for data block
                final long beginSeconds = dataBlock.getBeginTime().getSeconds();
                final long beginNanos = dataBlock.getBeginTime().getNanos();
                final Instant beginInstant = Instant.ofEpochSecond(beginSeconds, beginNanos);
                final long endSeconds = dataBlock.getEndTime().getSeconds();
                final long endNanos = dataBlock.getEndTime().getNanos();
                final Instant endInstant = Instant.ofEpochSecond(endSeconds, endNanos);

                // update min/max time range for dataset
                if (first) {
                    exportBeginInstant = beginInstant;
                    exportEndInstant = endInstant;
                    first = false;
                } else {
                    if (beginInstant.isBefore(exportBeginInstant)) {
                        exportBeginInstant = beginInstant;
                    }
                    if (endInstant.isAfter(exportEndInstant)) {
                        exportEndInstant = endInstant;
                    }
                }

                TabularDataUtility.TimestampDataMapSizeStats sizeStats = null;
                try {
                    sizeStats = TabularDataUtility.addBucketsToTable(
                            tableValueMap,
                            cursor,
                            tableDataSize,
                            ExportConfiguration.getExportFileSizeLimitBytes(),
                            beginSeconds,
                            beginNanos,
                            endSeconds,
                            endNanos
                    );
                } catch (DpException e) {
                    final String errorMsg = "exception building tabular result: " + e.getMessage();
                    logger.error("id: {}, error: {}", this.handlerRequest.responseObserver.hashCode(), errorMsg);
                    return new ExportDataStatus(true, errorMsg);
                }

                // check if tabular structure execeeds export output file size limit
                if (sizeStats.sizeLimitExceeded()) {
                    final String errorMsg = "export file size limit "
                            + ExportConfiguration.getExportFileSizeLimitBytes()
                            + " exceeded for: " + serverFilePath;
                    logger.trace("id: {}, error: {}", this.handlerRequest.responseObserver.hashCode(), errorMsg);
                    return new ExportDataStatus(true, errorMsg);
                }

                tableDataSize = tableDataSize + sizeStats.currentDataSize();
            }
        }

        // add calculations to tabular data structure
        if (calculationsDocument != null) {
            TabularDataUtility.TimestampDataMapSizeStats sizeStats;
            try {
                sizeStats =
                        TabularDataUtility.addCalculationsToTable(
                                tableValueMap,
                                calculationsDocument,
                                frameColumnNamesMap,
                                exportBeginInstant,
                                exportEndInstant,
                                tableDataSize,
                                ExportConfiguration.getExportFileSizeLimitBytes());
            } catch (DpException e) {
                final String errorMsg = "Exception adding calculations to table for file: "
                        + serverFilePath;
                logger.error("id: {}, error: {}", this.handlerRequest.responseObserver.hashCode(), errorMsg);
                return new ExportDataStatus(true, errorMsg);
            }
            if (sizeStats.sizeLimitExceeded()) {
                final String errorMsg = "export file size limit "
                        + ExportConfiguration.getExportFileSizeLimitBytes()
                        + " exceeded for: " + serverFilePath;
                logger.error("id: {}, error: {}", this.handlerRequest.responseObserver.hashCode(), errorMsg);
                return new ExportDataStatus(true, errorMsg);
            }
        }

        // write column headers to output file
        final List<String> columnHeaders = new ArrayList<>();
        columnHeaders.add(COLUMN_HEADER_SECONDS);
        columnHeaders.add(COLUMN_HEADER_NANOS);
        columnHeaders.addAll(tableValueMap.getColumnNameList());
        try {
            exportFile.writeHeaderRow(columnHeaders);
        } catch (DpException e) {
            final String errorMsg = "exception writing header to export file " + serverFilePath + ": " + e.getMessage();
            logger.error("id: {}, error: {}", this.handlerRequest.responseObserver.hashCode(), errorMsg);
            return new ExportDataStatus(true, errorMsg);
        }

        // write data to output file
        try {
            exportFile.writeData(tableValueMap);
        } catch (DpException e) {
            final String errorMsg = "exception writing data to export file " + serverFilePath + ": " + e.getMessage();
            logger.error("id: {}, error: {}", this.handlerRequest.responseObserver.hashCode(), errorMsg);
            return new ExportDataStatus(true, errorMsg);
        }

        // close output file
        try {
            exportFile.close();
        } catch (DpException e) {
            final String errorMsg = "exception closing export file " + serverFilePath + ": " + e.getMessage();
            logger.error("id: {}, error: {}", this.handlerRequest.responseObserver.hashCode(), errorMsg);
            return new ExportDataStatus(true, errorMsg);
        }

        return new ExportDataStatus(false, "");
    }

}
