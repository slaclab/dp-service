package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.ospreydcs.dp.service.annotation.handler.model.ExportConfiguration;
import com.ospreydcs.dp.service.annotation.handler.model.HandlerExportDataRequest;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.export.DataExportHdf5File;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;

/**
 * This class services an exportData() request using a bucketed HDF5 output format.  It creates a DataExportHdf5File
 * object for use by ExportDataJobAbstractBucketed.execute() to write the output file.
 */
public class ExportDataJobHdf5 extends ExportDataJobAbstractBucketed {

    public ExportDataJobHdf5(
            HandlerExportDataRequest handlerRequest,
            MongoAnnotationClientInterface mongoAnnotationClient,
            MongoQueryClientInterface mongoQueryClient
    ) {
        super(handlerRequest, mongoAnnotationClient, mongoQueryClient);
    }

    protected String getFileExtension_() {
        return ExportConfiguration.FILE_EXTENSION_HDF5;
    }

    protected DataExportHdf5File createExportFile_(String serverFilePath) throws DpException {

        return new DataExportHdf5File(serverFilePath);
    }

}
