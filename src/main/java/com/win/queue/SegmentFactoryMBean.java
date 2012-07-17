package com.win.queue;

public interface SegmentFactoryMBean {
    public int getInActiveSegments();

    public int getActiveSegments();

    public long getTotalReservedBytes();

    public String getCurrentSegmentName();
}
