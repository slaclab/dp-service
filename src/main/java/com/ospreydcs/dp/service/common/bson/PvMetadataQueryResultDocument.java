package com.ospreydcs.dp.service.common.bson;

import java.util.Date;

public class PvMetadataQueryResultDocument {

    // instance variables
    private String pvName;
    private String lastBucketId;
    private String lastBucketDataType;
    private int lastBucketDataTimestampsCase;
    private String lastBucketDataTimestampsType;
    private int lastBucketSampleCount;
    private long lastBucketSamplePeriod;
    private Date firstDataTimestamp;
    private Date lastDataTimestamp;
    private int numBuckets;
    private String lastProviderId;
    private String lastProviderName;

    public String getPvName() {
        return pvName;
    }

    public void setPvName(String pvName) {
        this.pvName = pvName;
    }

    public String getLastBucketId() {
        return lastBucketId;
    }

    public void setLastBucketId(String lastBucketId) {
        this.lastBucketId = lastBucketId;
    }

    public String getLastBucketDataType() {
        return lastBucketDataType;
    }

    public void setLastBucketDataType(String lastBucketDataType) {
        this.lastBucketDataType = lastBucketDataType;
    }

    public int getLastBucketDataTimestampsCase() {
        return lastBucketDataTimestampsCase;
    }

    public void setLastBucketDataTimestampsCase(int lastBucketDataTimestampsCase) {
        this.lastBucketDataTimestampsCase = lastBucketDataTimestampsCase;
    }

    public String getLastBucketDataTimestampsType() {
        return lastBucketDataTimestampsType;
    }

    public void setLastBucketDataTimestampsType(String lastBucketDataTimestampsType) {
        this.lastBucketDataTimestampsType = lastBucketDataTimestampsType;
    }

    public int getLastBucketSampleCount() {
        return lastBucketSampleCount;
    }

    public void setLastBucketSampleCount(int lastBucketSampleCount) {
        this.lastBucketSampleCount = lastBucketSampleCount;
    }

    public long getLastBucketSamplePeriod() {
        return lastBucketSamplePeriod;
    }

    public void setLastBucketSamplePeriod(long lastBucketSamplePeriod) {
        this.lastBucketSamplePeriod = lastBucketSamplePeriod;
    }

    public Date getFirstDataTimestamp() {
        return firstDataTimestamp;
    }

    public void setFirstDataTimestamp(Date firstDataTimestamp) {
        this.firstDataTimestamp = firstDataTimestamp;
    }

    public Date getLastDataTimestamp() {
        return lastDataTimestamp;
    }

    public void setLastDataTimestamp(Date lastDataTimestamp) {
        this.lastDataTimestamp = lastDataTimestamp;
    }

    public int getNumBuckets() {
        return numBuckets;
    }

    public void setNumBuckets(int numBuckets) {
        this.numBuckets = numBuckets;
    }

    public String getLastProviderId() {
        return lastProviderId;
    }

    public void setLastProviderId(String lastProviderId) {
        this.lastProviderId = lastProviderId;
    }

    public String getLastProviderName() {
        return lastProviderName;
    }

    public void setLastProviderName(String lastProviderName) {
        this.lastProviderName = lastProviderName;
    }
}
