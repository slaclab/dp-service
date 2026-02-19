package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.common.CalculationsSpec;
import com.ospreydcs.dp.service.annotation.handler.model.HandlerExportDataRequest;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.export.BucketedDataExportFileInterface;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.calculations.CalculationsDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataBlockDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;

import java.util.Map;

/**
 * This is the abstract intermediate base class for bucketed data export formats (formats that handle one
 * bucket at a time, as opposed to a tabular output format).  This class
 * overrides the abstract method exportData_() to export the dataset and calculations specified in the request to the
 * bucketed output driver.  Instead of defining abstract methods for subclasses, the BucketedDataExportFileInterface
 * is used to specify the required interface for the concrete bucketed output classes.  Derived classes override
 * createExportFile_ to initialize the instance implementing BucketedDataExportFileInterface.
 */
public abstract class ExportDataJobAbstractBucketed extends ExportDataJobBase {

    // instance variables
    private BucketedDataExportFileInterface exportFile;

    public ExportDataJobAbstractBucketed(
            HandlerExportDataRequest handlerRequest,
            MongoAnnotationClientInterface mongoAnnotationClient,
            MongoQueryClientInterface mongoQueryClient
    ) {
        super(handlerRequest, mongoAnnotationClient, mongoQueryClient);
    }

    protected abstract BucketedDataExportFileInterface createExportFile_(
            String serverFilePath) throws DpException;

    @Override
    protected ExportDataStatus exportData_(
            DataSetDocument datasetDocument,
            CalculationsDocument calculationsDocument,
            Map<String, CalculationsSpec.ColumnNameList> frameColumnNamesMap,
            String serverFilePath
    ) {
        // create file for export
        try {
            exportFile = createExportFile_(serverFilePath);
        } catch (DpException e) {
            final String errorMsg = "exception opening export file " + serverFilePath + ": " + e.getMessage();
            logger.error("id: {}, error: {}", this.handlerRequest.responseObserver.hashCode(), errorMsg);
            return new ExportDataStatus(true, errorMsg);
        }

        // retrieve data blocks for dataset and write data to output hdf5 file
        if (datasetDocument != null) {

            // write dataset to output file
            exportFile.writeDataSet(datasetDocument);

            for (DataBlockDocument dataBlock : datasetDocument.getDataBlocks()) {

                final MongoCursor<BucketDocument> cursor =
                        this.mongoQueryClient.executeDataBlockQuery(dataBlock);

                if (cursor == null) {
                    final String errorMsg = "unknown error executing data block query";
                    logger.error("id: {}, error: {}", this.handlerRequest.responseObserver.hashCode(), errorMsg);
                    return new ExportDataStatus(true, errorMsg);
                }

                // We could enforce an export output file size limit here, as in ExportDataJobAbstractTabular.exportDataset_(),
                // but at this point I'm not going to do so.  In the case of tabular data, we build a large table data
                // structure before we write the data to file.  But with bucketed data (at least for HDF5 format), we write
                // each individual bucket to the file as we read it from the database cursor.  As HDF5 is designed to handle
                // large amounts of data, and we are considering its use as a long term archive format, I've decided not
                // to add constraints for the file size.  If we wanted to do so, we would probably change the signature of
                // writeBucketData() to include parameters for the previous file size and size limit, and return a structure
                // with the new file size and a flag indicating if the size limit is exceeded.  In terms of
                // the implementation in DataExportHdf5File.writeBucketData(), we could use
                // dataColumnBytes.getSerializedSize() to get the size of the protobuf vector of data values written to the
                // file for each bucket.

                while (cursor.hasNext()) {
                    final BucketDocument bucketDocument = cursor.next();
                    try {
                        exportFile.writeBucket(bucketDocument);
                    } catch (DpException e) {
                        final String errorMsg =
                                "exception writing data bucket to export file " + serverFilePath + ": " + e.getMessage();
                        logger.error("id: {}, error: {}", this.handlerRequest.responseObserver.hashCode(), errorMsg);
                        return new ExportDataStatus(true, errorMsg);
                    }
                }
            }
        }

        // write calculations to output file
        if (calculationsDocument != null) {
            try {
                exportFile.writeCalculations(calculationsDocument, frameColumnNamesMap);
            } catch (DpException e) {
                final String errorMsg =
                        "exception writing calculations to export file " + serverFilePath + ": " + e.getMessage();
                logger.error("id: {}, error: {}", this.handlerRequest.responseObserver.hashCode(), errorMsg);
                return new ExportDataStatus(true, errorMsg);
            }
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
