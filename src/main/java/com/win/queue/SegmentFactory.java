package com.win.queue;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * Segment Factory which manages multiple chunks of the filesystem and reuses
 * them to avoid leaks.
 * 
 * @author Vijay Parthasarathy
 */
public class SegmentFactory<E> implements SegmentFactoryMBean
{
    private static final String MBEAN_OBJECT_NAME = "com.win.queue:type=SegmentFactory,instance=";
    private final long fs_max; // 40 GB
    private final long segmentSize; // 128 MB

    private final ConcurrentLinkedQueue<Segment<E>> activeSegments = new ConcurrentLinkedQueue<Segment<E>>();
    private final ConcurrentLinkedQueue<Segment<E>> inActiveSegments = new ConcurrentLinkedQueue<Segment<E>>();
    private final File directory;
    private final QueueSerializer<E> serializer;
    private volatile Segment<E> currentSegment;

    public SegmentFactory(File directory, long segmentSize, long fsMax, QueueSerializer<E> serializer)
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(this, new ObjectName(MBEAN_OBJECT_NAME + hashCode()));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        this.segmentSize = segmentSize;
        this.directory = directory;
        this.serializer = serializer;
        this.fs_max = fsMax;
        newSegment(); // create the first segment.
    }

    public Segment<E> newSegment()
    {
        if (inActiveSegments.isEmpty() || inActiveSegments.poll().referenced)
        {
            if (getTotalReservedBytes() > fs_max)
                throw new RuntimeException("Queue Overflow, Increase the Max fs size or remove the elements from the queue.");
            currentSegment = new Segment<E>(directory, segmentSize, serializer);
            activeSegments.offer(currentSegment);
        }
        else
        {
            // remove from the inactive and move it to active.
            currentSegment = inActiveSegments.poll();
            activeSegments.add(currentSegment);
        }
        return currentSegment;
    }

    public Segment<E> getCurrent()
    {
        return currentSegment;
    }

    public Segment<E> next()
    {
        Segment<E> segment = activeSegments.peek();
        if (!segment.hasData() && activeSegments.size() > 1)
        {
            Segment<E> seg = activeSegments.poll();
            if (getTotalReservedBytes() > fs_max && !seg.referenced)
                seg.discard();
            inActiveSegments.offer(seg.recycle());
        }
        return activeSegments.peek();
    }

    public Queue<Segment<E>> cloneActive()
    {
        ArrayDeque<Segment<E>> q = new ArrayDeque<Segment<E>>(activeSegments.size());
        for (Segment<E> segment : activeSegments)
        {
            segment.referenced = true;
            q.add(segment);
        }
        return q;
    }

    public int getActiveSegments()
    {
        return activeSegments.size();
    }

    public int getInActiveSegments()
    {
        return inActiveSegments.size();
    }

    public String getCurrentSegmentName()
    {
        return currentSegment.getName();
    }

    public long getTotalReservedBytes()
    {
        return (inActiveSegments.size() + activeSegments.size()) * segmentSize;
    }

    public void clear()
    {
        for (int i = 1; i < activeSegments.size(); i++)
            inActiveSegments.offer(activeSegments.peek().recycle());
        currentSegment = currentSegment.recycle();
    }

    @Override
    public String toString()
    {
        return String.format("directory: %s, total space used: %d, inactive space: %d, active space: %d", directory.getPath(), 
                                getTotalReservedBytes() * segmentSize, 
                                getInActiveSegments() * segmentSize, 
                                getActiveSegments() * segmentSize);
    }
}
