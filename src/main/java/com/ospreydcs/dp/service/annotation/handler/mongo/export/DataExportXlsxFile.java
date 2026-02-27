package com.ospreydcs.dp.service.annotation.handler.mongo.export;

import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.model.TimestampDataMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.*;
import java.util.List;

/**
 * This class provides an interface for writing data to an Excel / XLSX output file.  It is created and managed by
 * ExportDataJobExcel and ExportDataJobAbstractTabular, respectively.
 *
 * It implements the TabularDataExportFileInterface methods, writeHeaderRow(), writeData(), and close().  The methods
 * use the org.apache.poi.xssf library for creating and writing the xlsx file.
 */
public class DataExportXlsxFile implements TabularDataExportFileInterface {

    // constants
    private final String SHEET_NAME_DATA = "data";

    // static variables
    protected static final Logger logger = LogManager.getLogger();

    // instance variables
    private final String filePathString;
    private final XSSFWorkbook workbook;
    private final Sheet dataSheet;
    private final CreationHelper creationHelper;
    private int currentDataRowIndex = 1;

    public DataExportXlsxFile(DataSetDocument dataSet, String filePathString) throws DpException {
        this.filePathString = filePathString;
        this.workbook = new XSSFWorkbook(); // Use non-streaming workbook for better reliability
        this.dataSheet = workbook.createSheet(SHEET_NAME_DATA);
        this.creationHelper = workbook.getCreationHelper();
    }

    @Override
    public void writeHeaderRow(List<String> headers) throws DpException {
        if (headers == null || headers.isEmpty()) {
            throw new DpException("Headers list cannot be null or empty");
        }
        final Row headerRow = dataSheet.createRow(0);
        for (int i = 0; i < headers.size(); i++) {
            final Cell cell = headerRow.createCell(i);
            String headerValue = headers.get(i);
            cell.setCellValue(headerValue != null ? headerValue : "");
        }
    }

    private void setCellValue(Cell fileDataCell, DataValue dataValue) {
        if (fileDataCell == null || dataValue == null) {
            return; // Skip null cells or values
        }

        switch (dataValue.getValueCase()) {
            case STRINGVALUE -> {
                String strValue = dataValue.getStringValue();
                fileDataCell.setCellValue(strValue != null ? strValue : "");
            }
            case BOOLEANVALUE -> {
                fileDataCell.setCellValue(dataValue.getBooleanValue());
            }
            case UINTVALUE -> {
                // Convert unsigned int to double to avoid Excel issues with large unsigned values
                fileDataCell.setCellValue((double) dataValue.getUintValue());
            }
            case ULONGVALUE -> {
                // Convert unsigned long to double to avoid Excel issues with large unsigned values
                fileDataCell.setCellValue((double) dataValue.getUlongValue());
            }
            case INTVALUE -> {
                fileDataCell.setCellValue(dataValue.getIntValue());
            }
            case LONGVALUE -> {
                fileDataCell.setCellValue(dataValue.getLongValue());
            }
            case FLOATVALUE -> {
                fileDataCell.setCellValue(dataValue.getFloatValue());
            }
            case DOUBLEVALUE -> {
                fileDataCell.setCellValue(dataValue.getDoubleValue());
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
                fileDataCell.setCellValue(dataValue.toString());
            }
        }
    }

    @Override
    public void writeData(TimestampDataMap timestampDataMap) throws DpException {
        if (timestampDataMap == null) {
            logger.warn("TimestampDataMap is null, skipping data write");
            return;
        }
        final TimestampDataMap.DataRowIterator dataRowIterator = timestampDataMap.dataRowIterator();
        while (dataRowIterator.hasNext()) {
            final TimestampDataMap.DataRow sourceDataRow = dataRowIterator.next();
            final Row fileDataRow = dataSheet.createRow(currentDataRowIndex);
            fileDataRow.createCell(0).setCellValue(sourceDataRow.seconds());
            fileDataRow.createCell(1).setCellValue(sourceDataRow.nanos());
            int dataColumnIndex = 2;
            for (DataValue sourceCellValue : sourceDataRow.dataValues()) {
                Cell fileDataCell = fileDataRow.createCell(dataColumnIndex);
                setCellValue(fileDataCell, sourceCellValue);
                dataColumnIndex++;
            }

            this.currentDataRowIndex++;
        }
    }

    @Override
    public void close() throws DpException {
        try (FileOutputStream fileOutputStream = new FileOutputStream(new File(filePathString))) {
            this.workbook.write(fileOutputStream);
            fileOutputStream.flush();
            logger.debug("wrote excel workbook to: " + filePathString);
        } catch (IOException e) {
            logger.error("IOException writing excel file: " + e.getMessage());
            throw new DpException(e);
        } finally {
            // Close the workbook (XSSFWorkbook doesn't need dispose())
            try {
                this.workbook.close();
                logger.debug("closed excel workbook for: " + filePathString);
            } catch (IOException e) {
                logger.error("IOException closing workbook: " + e.getMessage());
                throw new DpException(e);
            }
        }
    }
}
