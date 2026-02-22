package com.ospreydcs.dp.service.common.bson.column;

import org.bson.types.ObjectId;

/**
 * POJO for representing storage details of binary data in MongoDB.
 * Supports both inline storage (data embedded in document) and GridFS storage (data in separate GridFS files).
 * This abstraction allows the same document structure to handle both small data (inline) and large data (GridFS).
 */
public class StorageDocument {

    public enum StorageKind {
        INLINE,
        GRIDFS
    }

    private StorageKind kind;
    private byte[] data;        // Used when kind = INLINE
    private ObjectId fileId;    // Used when kind = GRIDFS
    private Long sizeBytes;     // Optional but useful for both storage types

    public StorageDocument() {
    }

    /**
     * Creates a StorageDocument for inline storage with binary data.
     */
    public static StorageDocument inline(byte[] data) {
        StorageDocument storage = new StorageDocument();
        storage.setKind(StorageKind.INLINE);
        storage.setData(data);
        storage.setSizeBytes((long) data.length);
        return storage;
    }

    /**
     * Creates a StorageDocument for GridFS storage with file reference.
     */
    public static StorageDocument gridfs(ObjectId fileId, long sizeBytes) {
        StorageDocument storage = new StorageDocument();
        storage.setKind(StorageKind.GRIDFS);
        storage.setFileId(fileId);
        storage.setSizeBytes(sizeBytes);
        return storage;
    }

    public StorageKind getKind() {
        return kind;
    }

    public void setKind(StorageKind kind) {
        this.kind = kind;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public ObjectId getFileId() {
        return fileId;
    }

    public void setFileId(ObjectId fileId) {
        this.fileId = fileId;
    }

    public Long getSizeBytes() {
        return sizeBytes;
    }

    public void setSizeBytes(Long sizeBytes) {
        this.sizeBytes = sizeBytes;
    }

    /**
     * Returns true if this storage uses inline data embedding.
     */
    public boolean isInline() {
        return kind == StorageKind.INLINE;
    }

    /**
     * Returns true if this storage uses GridFS file reference.
     */
    public boolean isGridfs() {
        return kind == StorageKind.GRIDFS;
    }
}