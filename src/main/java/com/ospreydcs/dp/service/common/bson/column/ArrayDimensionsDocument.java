package com.ospreydcs.dp.service.common.bson.column;

import com.ospreydcs.dp.grpc.v1.common.ArrayDimensions;

import java.util.List;

/**
 * POJO for representing ArrayDimensions in MongoDB.
 * Encapsulates the dimensional information for array column types.
 */
public class ArrayDimensionsDocument {

    private List<Integer> dims;

    public ArrayDimensionsDocument() {
    }

    public List<Integer> getDims() {
        return dims;
    }

    public void setDims(List<Integer> dims) {
        this.dims = dims;
    }

    /**
     * Creates an ArrayDimensionsDocument from a protobuf ArrayDimensions message.
     */
    public static ArrayDimensionsDocument fromArrayDimensions(ArrayDimensions arrayDimensions) {
        ArrayDimensionsDocument document = new ArrayDimensionsDocument();
        // Convert from List<Integer> (protobuf uint32) to List<Integer>
        document.setDims(arrayDimensions.getDimsList());
        return document;
    }

    /**
     * Converts this document to a protobuf ArrayDimensions message.
     */
    public ArrayDimensions toArrayDimensions() {
        ArrayDimensions.Builder builder = ArrayDimensions.newBuilder();
        if (dims != null) {
            builder.addAllDims(dims);
        }
        return builder.build();
    }

    /**
     * Calculates the total number of elements in one array sample.
     * This is the product of all dimension values.
     */
    public int getElementCount() {
        if (dims == null || dims.isEmpty()) {
            return 0;
        }
        
        int product = 1;
        for (int dim : dims) {
            product *= dim;
        }
        return product;
    }

    /**
     * Returns the number of dimensions (1, 2, or 3).
     */
    public int getDimensionCount() {
        return dims != null ? dims.size() : 0;
    }
}