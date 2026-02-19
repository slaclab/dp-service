package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.ospreydcs.dp.grpc.v1.common.CalculationsSpec;
import com.ospreydcs.dp.service.annotation.handler.model.ExportConfiguration;
import com.ospreydcs.dp.service.annotation.handler.model.HandlerExportDataRequest;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.dispatch.ExportDataDispatcher;
import com.ospreydcs.dp.service.common.bson.calculations.CalculationsDataFrameDocument;
import com.ospreydcs.dp.service.common.bson.calculations.CalculationsDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * This is the base class for Annotation Service handler jobs to service incoming exportData() API requests.  There is
 * a class hierarchy that corresponds to the supported export output file formats.  There are intermediate base classes
 * for handling tabular (e.g., csv and xlsx) formats, and "bucket-oriented" (e.g., hdf5) formats, with concrete derived
 * classes for the specific formats.
 *
 * The execute() method in this class is the main driver for the hierarchy, with subclasses implementing exportData_() to
 * handle the actual export.
 */
public abstract class ExportDataJobBase extends HandlerJob {

    protected static record ExportDataStatus(boolean isError, String errorMessage) {}

    // static variables
    protected static final Logger logger = LogManager.getLogger();

    // instance variables
    protected final HandlerExportDataRequest handlerRequest;
    private final ExportDataDispatcher dispatcher;
    private final MongoAnnotationClientInterface mongoAnnotationClient;
    protected final MongoQueryClientInterface mongoQueryClient;
    private final ExportConfiguration exportConfiguration;

    public ExportDataJobBase(
            HandlerExportDataRequest handlerRequest,
            MongoAnnotationClientInterface mongoAnnotationClient,
            MongoQueryClientInterface mongoQueryClient
    ) {
        this.handlerRequest = handlerRequest;
        this.mongoAnnotationClient = mongoAnnotationClient;
        this.mongoQueryClient = mongoQueryClient;
        this.dispatcher = new ExportDataDispatcher(handlerRequest, mongoAnnotationClient);
        this.exportConfiguration = new ExportConfiguration();
    }

    protected abstract String getFileExtension_();
    protected abstract ExportDataStatus exportData_(
            DataSetDocument datasetDocument,
            CalculationsDocument calculationsDocument,
            Map<String, CalculationsSpec.ColumnNameList> frameColumnNamesMap,
            String serverFilePath);

    /**
     * This is the common driver for the export job class hierarchy.  It retrieves the dataset and calculations objects
     * specified in the request, and verifies the filtering of calculations columns.  It creates the server file, and
     * invokes the abstract method exportData_() to export data for the dataset and collections in the request.  Results
     * are sent in the API method response stream via the dispatcher.
     */
    @Override
    public void execute() {

        logger.debug(
                "executing {} id: {}",
                this.getClass().getSimpleName(),
                this.handlerRequest.responseObserver.hashCode());

        // use datasetId as filename if specified, otherwise use id from calculationsSpec
        String exportObjectId = null;

        // get dataset for id specified in request
        final String datasetId = this.handlerRequest.exportDataRequest.getDataSetId();
        DataSetDocument datasetDocument = null;
        if ( ! datasetId.isBlank()) {
            datasetDocument = mongoAnnotationClient.findDataSet(datasetId);
            if (datasetDocument == null) {
                final String errorMsg = "DatasetDocument with id " + datasetId + " not found";
                this.dispatcher.handleError(errorMsg);
                return;
            }
            exportObjectId = datasetId;
        }

        // get calculations for id specified in request
        CalculationsDocument calculationsDocument = null;
        Map<String, CalculationsSpec.ColumnNameList> requestFrameColumnNamesMap = null;
        if (this.handlerRequest.exportDataRequest.hasCalculationsSpec()) {
            final CalculationsSpec calculationsSpec = this.handlerRequest.exportDataRequest.getCalculationsSpec();
            final String calculationsId = calculationsSpec.getCalculationsId();
            requestFrameColumnNamesMap = calculationsSpec.getDataFrameColumnsMap().isEmpty() ?
                    null : calculationsSpec.getDataFrameColumnsMap();
            calculationsDocument = mongoAnnotationClient.findCalculations(calculationsId);
            if (calculationsDocument == null) {
                final String errorMsg = "CalculationsDocument with id " + calculationsId + " not found";
                this.dispatcher.handleError(errorMsg);
                return;
            }
            if (exportObjectId == null) {
                exportObjectId = calculationsId;
            }
        }

        // validate request frameColumnNamesMap: is each frame and column name valid for the specified calculations object?
        if (requestFrameColumnNamesMap != null) {
            final Map<String, List<String>> documentFrameColumnNamesMap =
                    calculationsDocument.frameColumnNamesMap();
            for (var requestMapEntry : requestFrameColumnNamesMap.entrySet()) {

                final String requestFrameName = requestMapEntry.getKey();
                final List<String> requestFrameColumnNames = requestMapEntry.getValue().getColumnNamesList();

                // validate frame name
                if ( ! documentFrameColumnNamesMap.containsKey(requestFrameName)) {
                    final String errorMsg =
                            "ExportDataRequest.CalculationsSpec.dataFrameColumns includes invalid frame name: "
                                    + requestFrameName;
                    this.dispatcher.handleError(errorMsg);
                    return;
                } else {

                    // validate frame column names
                    final List<String> documentFrameColumnNames = documentFrameColumnNamesMap.get(requestFrameName);
                    for (String requestFrameColumnName : requestFrameColumnNames) {
                        if ( ! documentFrameColumnNames.contains(requestFrameColumnName)) {
                            final String errorMsg =
                                    "ExportDataRequest.CalculationsSpec.dataFrameColumns includes invalid column name: "
                                            + requestFrameColumnName
                                            + " for frame: "
                                            + requestFrameName;
                            this.dispatcher.handleError(errorMsg);
                            return;
                        }
                    }
                }
            }
        }

        // make sure we have an id to use for filename
        if (exportObjectId == null) {
            final String errorMsg = "Unable to generate export output filename for request";
            this.dispatcher.handleError(errorMsg);
            return;
        }

        // check that there is data to export
        if (datasetDocument == null && calculationsDocument == null) {
            // this is unexpected, given prior validation
            final String errorMsg = "no data found for export request";
            logger.error(errorMsg + " id: " + this.handlerRequest.responseObserver.hashCode());
            this.dispatcher.handleError(errorMsg);
            return;
        }

        // generate server output file path for export
        final ExportConfiguration.ExportFilePaths exportFilePaths =
                exportConfiguration.getExportFilePaths(exportObjectId, getFileExtension_());
        if (! exportFilePaths.valid) {
            final String errorMsg =
                    "Export mechanism is not properly configured (e.g., see resources/application.yml file)";
            this.dispatcher.handleError(errorMsg);
            return;
        }
        final String serverDirectoryPathString = exportFilePaths.serverDirectoryPath;
        final String filename = exportFilePaths.filename;

        // create directories in server file path
        final Path serverDirectoryPath = Paths.get(serverDirectoryPathString);
        try {
            Files.createDirectories(serverDirectoryPath);
        } catch (IOException e) {
            final String errorMsg =
                    "IOException creating directories in path " + serverDirectoryPathString + ": " + e.getMessage();
            logger.error(errorMsg + " id: " + this.handlerRequest.responseObserver.hashCode());
            this.dispatcher.handleError(errorMsg);
            return;
        }

        // export data to file
        final String serverFilePathString = serverDirectoryPathString + filename;
        final Path serverFilePath = Paths.get(serverFilePathString);
        final ExportDataStatus status = exportData_(
                datasetDocument, calculationsDocument, requestFrameColumnNamesMap, serverFilePathString);
        if (status.isError) {
            logger.error(status.errorMessage + " id: " + this.handlerRequest.responseObserver.hashCode());
            this.dispatcher.handleError(status.errorMessage);
            return;
        }

        // log file size and check if open
        final File serverFile = new File(serverFilePathString);
        logger.trace("export file " + serverFilePathString + " size: " + serverFile.length()
                + " id: " + this.handlerRequest.responseObserver.hashCode());

        // check that file is readable before sending API response
        // even though Java calls like write() and close() are synchronous, the OS handling is async
        // we encountered race conditions if not checking that file writing completes before sending api response
        try {
            final BasicFileAttributes exportFileAttributes = awaitFile(serverFilePath, 60*1000 /* 60 seconds */);
        } catch (IOException | InterruptedException e) {
            final String errorMsg = "exception waiting for export file " + serverFilePathString + ": " + e.getMessage();
            logger.error(errorMsg + " id: " + this.handlerRequest.responseObserver.hashCode());
            this.dispatcher.handleError(errorMsg);
            return;
        }

        logger.debug(
                "dispatching {} id: {}",
                this.getClass().getSimpleName(),
                this.handlerRequest.responseObserver.hashCode());
        dispatcher.handleResult(exportFilePaths);
    }

    public static BasicFileAttributes awaitFile(Path target, long timeout)
            throws IOException, InterruptedException
    {
        final Path name = target.getFileName();
        final Path targetDir = target.getParent();

        // If path already exists, return early
        try {
            logger.debug("ExportDataJobBase.awaitFile path " + target.toString() + " already exists");
            return Files.readAttributes(target, BasicFileAttributes.class);
        } catch (NoSuchFileException ex) {}

        logger.debug("ExportDataJobBase.awaitFile using WatchService to wait for file " + target.toString());
        final WatchService watchService = FileSystems.getDefault().newWatchService();
        try {
            final WatchKey watchKey = targetDir.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);
            // The file could have been created in the window between Files.readAttributes and Path.register
            try {
                return Files.readAttributes(target, BasicFileAttributes.class);
            } catch (NoSuchFileException ex) {}
            // The file is absent: watch events in parent directory
            WatchKey watchKey1 = null;
            boolean valid = true;
            do {
                long t0 = System.currentTimeMillis();
                watchKey1 = watchService.poll(timeout, TimeUnit.MILLISECONDS);
                if (watchKey1 == null) {
                    return null; // timed out
                }
                // Examine events associated with key
                for (WatchEvent<?> event: watchKey1.pollEvents()) {
                    Path path1 = (Path) event.context();
                    if (path1.getFileName().equals(name)) {
                        return Files.readAttributes(target, BasicFileAttributes.class);
                    }
                }
                // Did not receive an interesting event; re-register key to queue
                long elapsed = System.currentTimeMillis() - t0;
                timeout = elapsed < timeout? (timeout - elapsed) : 0L;
                valid = watchKey1.reset();
            } while (valid);
        } finally {
            watchService.close();
        }

        return null;
    }

}
