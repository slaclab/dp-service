package com.ospreydcs.dp.service.common.bson.column;

import com.google.protobuf.Message;
import com.ospreydcs.dp.service.common.exception.DpException;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;

/**
 * Abstract base class for column document types that store binary data.
 * This includes array columns (DoubleArrayColumn, FloatArrayColumn, etc.) and 
 * complex columns (ImageColumn, StructColumn, SerializedDataColumn).
 * 
 * Provides storage abstraction that supports both inline storage (data embedded in document)
 * and GridFS storage (data stored in separate GridFS files) for handling large payloads.
 */
@BsonDiscriminator
public abstract class BinaryColumnDocumentBase extends ColumnDocumentBase {

    private StorageDocument storage;

    public StorageDocument getStorage() {
        return storage;
    }

    public void setStorage(StorageDocument storage) {
        this.storage = storage;
    }

    /**
     * Retrieves the binary data from storage, regardless of whether it's inline or GridFS.
     * Subclasses should override this method to implement GridFS retrieval if needed.
     * Default implementation handles inline storage only.
     */
    public byte[] getBinaryData() throws DpException {
        if (storage == null) {
            throw new DpException("Storage not initialized");
        }
        
        if (storage.isInline()) {
            return storage.getData();
        } else if (storage.isGridfs()) {
            // TODO: Implement GridFS retrieval when GridFS support is added
            throw new DpException("GridFS storage retrieval not yet implemented");
        } else {
            throw new DpException("Unknown storage kind: " + storage.getKind());
        }
    }

    /**
     * Stores binary data using appropriate storage mechanism (inline vs GridFS).
     * For now, always uses inline storage. GridFS support will be added later.
     */
    protected void setBinaryData(byte[] data) throws DpException {
        if (data == null) {
            throw new DpException("Binary data cannot be null");
        }
        
        // For now, always use inline storage
        // TODO: Add size threshold check and GridFS storage when needed
        this.storage = StorageDocument.inline(data);
    }

    /**
     * Binary columns use direct deserialization pattern for protobuf conversion.
     * Each subclass implements its own deserialization logic.
     */
    @Override
    public final Message toProtobufColumn() {
        try {
            return deserializeToProtobufColumn();
        } catch (DpException e) {
            throw new RuntimeException("Failed to deserialize binary column", e);
        }
    }

    /**
     * Deserializes binary data to the appropriate protobuf column type.
     * Each binary column type implements its own deserialization logic.
     */
    protected abstract Message deserializeToProtobufColumn() throws DpException;
}