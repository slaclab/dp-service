package com.ospreydcs.dp.service.annotation.handler.mongo.export;

import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.model.TimestampDataMap;
import de.siegmar.fastcsv.writer.CsvWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * This class provides an interface for writing data to CSV output file.  It is created and managed by
 * ExportDataJobExcel and ExportDataJobAbstractTabular, respectively.
 *
 * It implements the TabularDataExportFileInterface methods, writeHeaderRow(), writeData(), and close(). It uses the
 * de.siegmar.fastcsv library for creating and writing to the CSV file.
 */
public class DataExportCsvFile implements TabularDataExportFileInterface {

    // static variables
    protected static final Logger logger = LogManager.getLogger();

    // instance variables
    private final CsvWriter csvWriter;

    public DataExportCsvFile(DataSetDocument dataSet, String filePathString) throws DpException {
        Path filePath = Paths.get(filePathString);
        try {
            this.csvWriter = CsvWriter.builder().build(filePath);
        } catch (IOException e) {
            throw new DpException(e);
        }
    }

    public static String dataValueToString(DataValue dataValue) {

        String columnValueString = "";
        switch (dataValue.getValueCase()) {
            case STRINGVALUE -> {
                columnValueString = dataValue.getStringValue();
            }
            case BOOLEANVALUE -> {
                columnValueString = Boolean.toString(dataValue.getBooleanValue());
            }
            case UINTVALUE -> {
                columnValueString = Integer.toString(dataValue.getUintValue());
            }
            case ULONGVALUE -> {
                columnValueString = Long.toString(dataValue.getLongValue());
            }
            case INTVALUE -> {
                columnValueString = Integer.toString(dataValue.getIntValue());
            }
            case LONGVALUE -> {
                columnValueString = Long.toString(dataValue.getLongValue());
            }
            case FLOATVALUE -> {
                columnValueString = Float.toString(dataValue.getFloatValue());
            }
            case DOUBLEVALUE -> {
                columnValueString = String.valueOf(dataValue.getDoubleValue());
            }
//            case BYTEARRAYVALUE -> {
//            }
//            case ARRAYVALUE -> {
//            }
//            case STRUCTUREVALUE -> {
//            }
//            case IMAGEVALUE -> {
//            }
//            case TIMESTAMPVALUE -> {
//            }
//            case VALUE_NOT_SET -> {
//            }
            default -> {
                columnValueString = dataValue.toString();
            }
        }
        return columnValueString;
    }

    @Override
    public void writeHeaderRow(List<String> headers) {
        this.csvWriter.writeRecord(headers);
    }

    @Override
    public void writeData(TimestampDataMap tableValueMap) {
        final TimestampDataMap.DataRowIterator dataRowIterator = tableValueMap.dataRowIterator();
        while (dataRowIterator.hasNext()) {
            final TimestampDataMap.DataRow dataRow = dataRowIterator.next();
            final List<String> rowDataValues = new ArrayList<>();
            rowDataValues.add(String.valueOf(dataRow.seconds()));
            rowDataValues.add(String.valueOf(dataRow.nanos()));
            for (DataValue columnDataValue : dataRow.dataValues()) {
                String columnValueString = dataValueToString(columnDataValue);
                rowDataValues.add(columnValueString);
            }
            this.csvWriter.writeRecord(rowDataValues);
        }
    }

    @Override
    public void close() {
        try {
            this.csvWriter.close();
        } catch (IOException e) {
            logger.error("IOException in CsvWriter.close(): " + e.getMessage());
        }
    }

}
