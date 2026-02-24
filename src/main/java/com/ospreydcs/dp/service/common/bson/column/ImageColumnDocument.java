package com.ospreydcs.dp.service.common.bson.column;

import com.google.protobuf.Message;
import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.common.ImageColumn;
import com.ospreydcs.dp.grpc.v1.common.ImageDescriptor;
import com.ospreydcs.dp.service.common.exception.DpException;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;

/**
 * BSON document class for ImageColumn storage in MongoDB.
 * Each ImageColumn contains an ImageDescriptor and a list of image values serialized as byte arrays.
 * Uses BinaryColumnDocumentBase for efficient storage of potentially large image payloads.
 */
@BsonDiscriminator(key = "_t", value = "imageColumn")
public class ImageColumnDocument extends BinaryColumnDocumentBase {

    private ImageDescriptorDocument imageDescriptor;

    public ImageDescriptorDocument getImageDescriptor() {
        return imageDescriptor;
    }

    public void setImageDescriptor(ImageDescriptorDocument imageDescriptor) {
        this.imageDescriptor = imageDescriptor;
    }

    /**
     * Helper class for storing ImageDescriptor as embedded BSON document.
     */
    public static class ImageDescriptorDocument {
        private int width;
        private int height;
        private int channels;
        private String encoding;

        public int getWidth() {
            return width;
        }

        public void setWidth(int width) {
            this.width = width;
        }

        public int getHeight() {
            return height;
        }

        public void setHeight(int height) {
            this.height = height;
        }

        public int getChannels() {
            return channels;
        }

        public void setChannels(int channels) {
            this.channels = channels;
        }

        public String getEncoding() {
            return encoding;
        }

        public void setEncoding(String encoding) {
            this.encoding = encoding;
        }

        public static ImageDescriptorDocument fromImageDescriptor(ImageDescriptor descriptor) {
            ImageDescriptorDocument doc = new ImageDescriptorDocument();
            doc.setWidth((int) descriptor.getWidth());
            doc.setHeight((int) descriptor.getHeight());
            doc.setChannels((int) descriptor.getChannels());
            doc.setEncoding(descriptor.getEncoding());
            return doc;
        }

        public ImageDescriptor toImageDescriptor() {
            return ImageDescriptor.newBuilder()
                    .setWidth(width)
                    .setHeight(height)
                    .setChannels(channels)
                    .setEncoding(encoding != null ? encoding : "")
                    .build();
        }
    }

    public static ImageColumnDocument fromImageColumn(ImageColumn requestColumn) throws DpException {
        ImageColumnDocument document = new ImageColumnDocument();
        document.setName(requestColumn.getName());
        
        // Convert ImageDescriptor
        document.setImageDescriptor(ImageDescriptorDocument.fromImageDescriptor(requestColumn.getImageDescriptor()));
        
        // Serialize image values to binary storage
        // Calculate total size needed for all image values
        int totalSize = 0;
        for (com.google.protobuf.ByteString imageValue : requestColumn.getImagesList()) {
            totalSize += Integer.BYTES; // 4 bytes for length prefix
            totalSize += imageValue.size(); // size of image data
        }
        
        // Create binary array with length-prefixed image values
        byte[] binaryData = new byte[totalSize];
        int offset = 0;
        
        for (com.google.protobuf.ByteString imageValue : requestColumn.getImagesList()) {
            byte[] imageBytes = imageValue.toByteArray();
            int imageSize = imageBytes.length;
            
            // Write length prefix (4 bytes, little-endian)
            binaryData[offset] = (byte) (imageSize & 0xFF);
            binaryData[offset + 1] = (byte) ((imageSize >> 8) & 0xFF);
            binaryData[offset + 2] = (byte) ((imageSize >> 16) & 0xFF);
            binaryData[offset + 3] = (byte) ((imageSize >> 24) & 0xFF);
            offset += 4;
            
            // Write image data
            System.arraycopy(imageBytes, 0, binaryData, offset, imageSize);
            offset += imageSize;
        }
        
        document.setBinaryData(binaryData);
        return document;
    }

    @Override
    protected Message.Builder createColumnBuilder() {
        // Image columns override toProtobufColumn() directly
        throw new UnsupportedOperationException("Image columns should override toProtobufColumn() directly");
    }

    @Override
    protected void addAllValuesToBuilder(Message.Builder builder) {
        // Not used by image columns - they override toProtobufColumn() directly
        throw new UnsupportedOperationException("Image columns should override toProtobufColumn() directly");
    }

    @Override
    public Message toProtobufColumn() {
        try {
            return deserializeToProtobufColumn();
        } catch (DpException e) {
            throw new RuntimeException("Failed to deserialize image column", e);
        }
    }

    protected Message deserializeToProtobufColumn() throws DpException {
        ImageColumn.Builder builder = ImageColumn.newBuilder();
        
        // Set name and imageDescriptor
        builder.setName(getName() != null ? getName() : "");
        if (getImageDescriptor() != null) {
            builder.setImageDescriptor(getImageDescriptor().toImageDescriptor());
        }
        
        // Deserialize image values from binary storage
        byte[] binaryData = getBinaryData();
        int offset = 0;
        
        while (offset < binaryData.length) {
            if (offset + 4 > binaryData.length) {
                throw new DpException("Invalid binary data: insufficient bytes for length prefix");
            }
            
            // Read length prefix (4 bytes, little-endian)
            int imageSize = (binaryData[offset] & 0xFF) |
                           ((binaryData[offset + 1] & 0xFF) << 8) |
                           ((binaryData[offset + 2] & 0xFF) << 16) |
                           ((binaryData[offset + 3] & 0xFF) << 24);
            offset += 4;
            
            if (offset + imageSize > binaryData.length) {
                throw new DpException("Invalid binary data: insufficient bytes for image data");
            }
            
            // Read image data
            byte[] imageBytes = new byte[imageSize];
            System.arraycopy(binaryData, offset, imageBytes, 0, imageSize);
            offset += imageSize;
            
            builder.addImages(com.google.protobuf.ByteString.copyFrom(imageBytes));
        }
        
        return builder.build();
    }

    @Override
    public void addColumnToBucket(DataBucket.Builder bucketBuilder) throws DpException {
        ImageColumn imageColumn = (ImageColumn) toProtobufColumn();
        bucketBuilder.setImageColumn(imageColumn);
    }
}