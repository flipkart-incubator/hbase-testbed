package com.flipkart.yaktest.models;

import java.util.ArrayList;
import java.util.List;

public class WalFileInfo {
    private String regionServer;
    private String fileName;
    private long bytes;
    private int numBlocks;

    private List<BlockInfo> blocks = new ArrayList<>();

    public String getRegionServer() {
        return regionServer;
    }

    public String getFileName() {
        return fileName;
    }

    public long getBytes() {
        return bytes;
    }

    public int getNumBlocks() {
        return numBlocks;
    }

    public List<BlockInfo> getBlocks() {
        return blocks;
    }

    public void setRegionServer(String regionServer) {
        this.regionServer = regionServer;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public void setBytes(long bytes) {
        this.bytes = bytes;
    }

    public void setNumBlocks(int numBlocks) {
        this.numBlocks = numBlocks;
    }

    public void setBlocks(List<BlockInfo> blocks) {
        this.blocks = blocks;
    }
}
