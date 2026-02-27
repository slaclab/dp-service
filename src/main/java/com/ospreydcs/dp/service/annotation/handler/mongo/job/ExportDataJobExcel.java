package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.ospreydcs.dp.service.annotation.handler.model.ExportConfiguration;
import com.ospreydcs.dp.service.annotation.handler.model.HandlerExportDataRequest;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.export.DataExportXlsxFile;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;

/**
 * This class services an exportData() request using a tabular XLSX / Excel output format.  It creates a DataExportCsvFile
 * object for use by ExportDataJobAbstractTabular.execute() to write the output file.
 */
public class ExportDataJobExcel extends ExportDataJobAbstractTabular {

    public ExportDataJobExcel(
            HandlerExportDataRequest handlerRequest,
            MongoAnnotationClientInterface mongoAnnotationClient,
            MongoQueryClientInterface mongoQueryClient
    ) {
        super(handlerRequest, mongoAnnotationClient, mongoQueryClient);
    }

    protected String getFileExtension_() {
        return ExportConfiguration.FILE_EXTENSION_XLSX;
    }

    protected DataExportXlsxFile createExportFile_(
            DataSetDocument dataset,
            String serverFilePath
    ) throws DpException {
        return new DataExportXlsxFile(dataset, serverFilePath);
    }

}
