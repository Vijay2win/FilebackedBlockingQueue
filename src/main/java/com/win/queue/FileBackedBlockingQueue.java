package com.win.queue;

import java.io.File;
import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * File Backed Blocking Queue. Similar to the LinkedBlockingQueue except that
 * there is an File component.
 * 
 * TODO Follow-up updates in github.
 * 
 * @author Vijay Parthasarathy
 */
public class FileBackedBlockingQueue<E> extends AbstractQueue<E> implements BlockingQueue<E>
{
    private final AtomicInteger count = new AtomicInteger(0);
    private final ReentrantLock readLock = new ReentrantLock();
    private final ReentrantLock writeLock = new ReentrantLock();
    private final Condition notEmpty = readLock.newCondition();

    @VisibleForTesting
    protected SegmentFactory<E> segments;

    private FileBackedBlockingQueue(Builder<E> builder)
    {
        segments = new SegmentFactory<E>(builder.directory, builder.segmentSize, builder.fs_size, builder.seralizer);
    }

    public static class Builder<E>
    {
        private File directory;
        private QueueSerializer<E> seralizer;
        private int segmentSize = 128 * 1024 * 1024; // 128 M
        private long fs_size = 40 * 1024 * 1024 * 1024; // 40G

        public Builder<E> directory(File directory)
        {
            this.directory = directory;
            return this;
        }

        public Builder<E> serializer(QueueSerializer<E> seralizer)
        {
            this.seralizer = seralizer;
            return this;
        }

        public Builder<E> segmentSize(int size)
        {
            this.segmentSize = size;
            return this;
        }

        /**
         * Maximum size of the queue in filesystem.
         */
        public Builder<E> max(long size)
        {
            assert fs_size > segmentSize;
            this.fs_size = size;
            return this;
        }

        public FileBackedBlockingQueue<E> build()
        {
            Preconditions.checkNotNull(directory);
            Preconditions.checkNotNull(seralizer);
            if (!directory.exists())
                throw new IllegalArgumentException("Directory for the file doesnt exist...");
            return new FileBackedBlockingQueue<E>(this);
        }
    }

    private void signalNotEmpty()
    {
        readLock.lock();
        try
        {
            notEmpty.signal();
        }
        finally
        {
            readLock.unlock();
        }
    }

    private void insert(E element)
    {
        if (!segments.getCurrent().hasCapacityFor(element))
            segments.newSegment();
        segments.getCurrent().add(element);
    }

    public int size()
    {
        return count.get();
    }

    public int remainingCapacity()
    {
        return Integer.MAX_VALUE;
    }

    public void put(E e) throws InterruptedException
    {
        Preconditions.checkNotNull(e);
        writeLock.lockInterruptibly();
        int c = -1;
        try
        {
            insert(e);
            c = count.getAndIncrement();
        }
        finally
        {
            writeLock.unlock();
        }
        if (c == 0)
            signalNotEmpty();
    }

    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException
    {
        // TODO measure the time to write to filesystem if any and then timeout
        // on that.
        return offer(e);
    }

    public boolean offer(E e)
    {
        Preconditions.checkNotNull(e);
        writeLock.lock();
        int c = -1;
        try
        {
            insert(e);
            c = count.incrementAndGet();
        }
        finally
        {
            writeLock.unlock();
        }
        if (c == 0)
            signalNotEmpty();
        return c >= 0;
    }

    public E take() throws InterruptedException
    {
        E element;
        readLock.lockInterruptibly();
        try
        {
            try
            {
                while (count.get() == 0)
                    notEmpty.await();
            }
            catch (InterruptedException ie)
            {
                notEmpty.signal(); // propagate to a non-interrupted thread
                throw ie;
            }
            element = extract();
            if (count.getAndDecrement() > 1)
                notEmpty.signal();
        }
        finally
        {
            readLock.unlock();
        }
        return element;
    }

    private E extract()
    {
        Segment<E> segment = null;
        while ((segment = segments.next()) != null)
        {
            if (!segment.hasData())
                return null;
            E element = null;
            if ((element = segment.read()) != null)
                return element;
        }
        return null;
    }

    public E poll(long timeout, TimeUnit unit) throws InterruptedException
    {
        E e = null;
        long nanos = unit.toNanos(timeout);
        readLock.lockInterruptibly();
        try
        {
            while (true)
            {
                if (count.get() > 0)
                {
                    e = extract();
                    if (count.getAndDecrement() > 1)
                        notEmpty.signal();
                    break;
                }
                if (nanos <= 0)
                    return null;
                try
                {
                    nanos = notEmpty.awaitNanos(nanos);
                }
                catch (InterruptedException ie)
                {
                    notEmpty.signal();
                    throw ie;
                }
            }
        }
        finally
        {
            readLock.unlock();
        }
        return e;
    }

    public E poll()
    {
        if (count.get() == 0)
            return null;
        E e = null;
        readLock.lock();
        try
        {
            if (count.get() > 0)
            {
                e = extract();
                if (count.getAndDecrement() > 1)
                    notEmpty.signal();
            }
        }
        finally
        {
            readLock.unlock();
        }
        return e;
    }

    public E peek()
    {
        if (count.get() == 0)
            return null;
        readLock.lock();
        try
        {
            Segment<E> segment = null;
            while ((segment = segments.next()) != null)
            {
                if (!segment.hasData())
                    return null;
                E element = null;
                if ((element = segment.readWithoutSeek()) != null)
                    return element;
            }
            return null;
        }
        finally
        {
            readLock.unlock();
        }
    }

    public boolean remove(Object o)
    {
        // TODO fix it. Iterate and mark it for remove.
        throw new UnsupportedOperationException();
    }

    public Object[] toArray()
    {
        // TODO fix it. Iterate and copy.
        throw new UnsupportedOperationException();
    }

    public <T> T[] toArray(T[] a)
    {
        // TODO fix it.
        throw new UnsupportedOperationException();
    }

    public String toString()
    {
        return String.format("FileBackedQueue %d, count: %d, %s", hashCode(), count.get(), segments.toString());
    }

    public void clear()
    {
        lockAll();
        try
        {
            count.set(0);
            segments.clear();
        }
        finally
        {
            unlockAll();
        }
    }

    public int drainTo(Collection<? super E> collection)
    {
        return drainTo(collection, Integer.MAX_VALUE);
    }

    public int drainTo(Collection<? super E> collection, int maxElements)
    {
        Preconditions.checkNotNull(collection);
        if (collection == this)
            throw new IllegalArgumentException();
        lockAll();
        try
        {
            int i = 0;
            for (; i < Math.min(count.get(), maxElements); i++)
            {
                count.decrementAndGet();
                collection.add(extract());
            }
            return i;
        }
        finally
        {
            unlockAll();
        }
    }

    private void lockAll()
    {
        readLock.lock();
        writeLock.lock();
    }

    private void unlockAll()
    {
        readLock.lock();
        writeLock.lock();
    }

    public Iterator<E> iterator()
    {
        // TODO fix it.
        throw new UnsupportedOperationException();
    }

    // private class ElementItrerator implements Iterator<E> {
    // private SegmentEntry current;
    // private SegmentEntry lastRet;
    // private E currentElement;
    //
    // ElementItrerator() {
    // writeLock.lock();
    // readLock.lock();
    // try {
    // current = head.next;
    // if (current != null)
    // currentElement = current.item;
    // } finally {
    // readLock.unlock();
    // writeLock.unlock();
    // }
    // }
    //
    // public boolean hasNext() {
    // return current != null;
    // }
    //
    // public E next() {
    // writeLock.lock();
    // readLock.lock();
    // try {
    // if (current == null)
    // throw new NoSuchElementException();
    // E x = currentElement;
    // lastRet = current;
    // current = current.next;
    // if (current != null)
    // currentElement = current.item;
    // return x;
    // } finally {
    // readLock.unlock();
    // writeLock.unlock();
    // }
    // }
    //
    // public void remove() {
    // if (lastRet == null)
    // throw new IllegalStateException();
    // writeLock.lock();
    // readLock.lock();
    // try {
    // SegmentEntry node = lastRet;
    // lastRet = null;
    // SegmentEntry trail = head;
    // SegmentEntry p = head.next;
    // while (p != null && p != node) {
    // trail = p;
    // p = p.next;
    // }
    // if (p == node) {
    // p.item = null;
    // trail.next = p.next;
    // if (last == p)
    // last = trail;
    // int c = count.getAndDecrement();
    // if (c == capacity)
    // notFull.signalAll();
    // }
    // } finally {
    // readLock.unlock();
    // writeLock.unlock();
    // }
    // }
    // }
}
