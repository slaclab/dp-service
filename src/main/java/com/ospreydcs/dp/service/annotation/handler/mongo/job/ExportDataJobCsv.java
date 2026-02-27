package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.ospreydcs.dp.service.annotation.handler.model.ExportConfiguration;
import com.ospreydcs.dp.service.annotation.handler.model.HandlerExportDataRequest;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.export.DataExportCsvFile;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;

/**
 * This class services an exportData() request using a tabular CSV output format.  It creates a DataExportCsvFile
 * object for use by ExportDataJobAbstractTabular.execute() to write the output file.
 */
public class ExportDataJobCsv extends ExportDataJobAbstractTabular {

    public ExportDataJobCsv(
            HandlerExportDataRequest handlerRequest,
            MongoAnnotationClientInterface mongoAnnotationClient,
            MongoQueryClientInterface mongoQueryClient
    ) {
        super(handlerRequest, mongoAnnotationClient, mongoQueryClient);
    }

    protected String getFileExtension_() {
        return ExportConfiguration.FILE_EXTENSION_CSV;
    }

    protected DataExportCsvFile createExportFile_(
            DataSetDocument dataset,
            String serverFilePath
    ) throws DpException {
        return new DataExportCsvFile(dataset, serverFilePath);
    }

}
