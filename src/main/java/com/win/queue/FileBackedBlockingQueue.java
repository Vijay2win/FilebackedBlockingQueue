package com.win.queue;

import java.io.File;
import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.win.queue.Segment.SegmentEntry;

/**
 * File Backed Blocking Queue. Similar to the LinkedBlockingQueue except that
 * there is an File component.
 * 
 * @author Vijay Parthasarathy
 */
public class FileBackedBlockingQueue<E> extends AbstractQueue<E> implements
	BlockingQueue<E> {
    private final AtomicInteger count = new AtomicInteger(0);
    private final ReentrantLock readLock = new ReentrantLock();
    private final ReentrantLock writeLock = new ReentrantLock();
    private final Condition notEmpty = readLock.newCondition();

    @VisibleForTesting
    protected SegmentFactory<E> segments;

    private FileBackedBlockingQueue(Builder<E> builder) {
	segments = new SegmentFactory<E>(builder.directory,
		builder.segmentSize, builder.fs_size, builder.seralizer);
    }

    public static class Builder<E> {
	private File directory;
	private QueueSerializer<E> seralizer;
	private long segmentSize = 128L * 1024 * 1024; // 128 M
	private long fs_size = 40L * 1024 * 1024 * 1024; // 40G

	/**
	 * Directory where the file based queue will reside.
	 */
	public Builder<E> directory(File directory) {
	    this.directory = directory;
	    return this;
	}

	/**
	 * Add serializer which will be used to read and write the objects to
	 * disk.
	 */
	public Builder<E> serializer(QueueSerializer<E> seralizer) {
	    this.seralizer = seralizer;
	    return this;
	}

	/**
	 * Segment size.
	 */
	public Builder<E> segmentSize(long size) {
	    this.segmentSize = size;
	    return this;
	}

	/**
	 * Maximum size of the queue in filesystem.
	 */
	public Builder<E> max(long size) {
	    assert fs_size > segmentSize;
	    this.fs_size = size;
	    return this;
	}

	public FileBackedBlockingQueue<E> build() {
	    Preconditions.checkNotNull(directory);
	    Preconditions.checkNotNull(seralizer);
	    if (!directory.exists())
		throw new IllegalArgumentException(
			"Directory for the file doesnt exist...");
	    return new FileBackedBlockingQueue<E>(this);
	}
    }

    private void signalNotEmpty() {
	readLock.lock();
	try {
	    notEmpty.signal();
	} finally {
	    readLock.unlock();
	}
    }

    private void insert(E element) {
	if (!segments.getCurrent().hasCapacityFor(element))
	    segments.newSegment();
	segments.getCurrent().add(element);
    }

    public int size() {
	return count.get();
    }

    public int remainingCapacity() {
	return Integer.MAX_VALUE;
    }

    public void put(E e) throws InterruptedException {
	offer(e);
    }

    public boolean offer(E e, long timeout, TimeUnit unit)
	    throws InterruptedException {
	// TODO measure the time to write to filesystem if any and then timeout
	// on that.
	return offer(e);
    }

    public boolean offer(E e) {
	Preconditions.checkNotNull(e);
	writeLock.lock();
	int c = -1;
	try {
	    insert(e);
	    c = count.incrementAndGet();
	} finally {
	    writeLock.unlock();
	}
	if (c == 0)
	    signalNotEmpty();
	return c >= 0;
    }

    public E take() throws InterruptedException {
	E element;
	readLock.lockInterruptibly();
	try {
	    try {
		while (count.get() == 0)
		    notEmpty.await();
	    } catch (InterruptedException ie) {
		notEmpty.signal(); // propagate to a non-interrupted thread
		throw ie;
	    }
	    element = extract();
	    if (count.getAndDecrement() > 1)
		notEmpty.signal();
	} finally {
	    readLock.unlock();
	}
	return element;
    }

    private E extract() {
	Segment<E> segment = null;
	while ((segment = segments.next()) != null) {
	    if (!segment.hasData())
		return null;
	    E element = null;
	    if ((element = segment.read()) != null)
		return element;
	}
	return null;
    }

    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
	E e = null;
	long nanos = unit.toNanos(timeout);
	readLock.lockInterruptibly();
	try {
	    while (true) {
		if (count.get() > 0) {
		    e = extract();
		    if (count.getAndDecrement() > 1)
			notEmpty.signal();
		    break;
		}
		if (nanos <= 0)
		    return null;
		try {
		    nanos = notEmpty.awaitNanos(nanos);
		} catch (InterruptedException ie) {
		    notEmpty.signal();
		    throw ie;
		}
	    }
	} finally {
	    readLock.unlock();
	}
	return e;
    }

    public E poll() {
	if (count.get() == 0)
	    return null;
	E e = null;
	readLock.lock();
	try {
	    if (count.get() > 0) {
		e = extract();
		if (count.getAndDecrement() > 1)
		    notEmpty.signal();
	    }
	} finally {
	    readLock.unlock();
	}
	return e;
    }

    public E peek() {
	if (count.get() == 0)
	    return null;
	readLock.lock();
	try {
	    Segment<E> segment = null;
	    while ((segment = segments.next()) != null) {
		if (!segment.hasData())
		    return null;
		E element = null;
		if ((element = segment.readWithoutSeek()) != null)
		    return element;
	    }
	    return null;
	} finally {
	    readLock.unlock();
	}
    }

    public boolean remove(Object o) {
	Preconditions.checkNotNull(o);
	CloseableIterator<E> it = iterator();
	while (it.hasNext()) {
	    E element = it.next();
	    if (element.equals(o)) {
		it.removeData();
		count.decrementAndGet();
		return true;
	    }
	}
	return false;
    }

    public String toString() {
	return String.format("FileBackedQueue %d, count: %d, %s", hashCode(),
		count.get(), segments.toString());
    }

    public void clear() {
	lockAll();
	try {
	    count.set(0);
	    segments.clear();
	} finally {
	    unlockAll();
	}
    }

    public int drainTo(Collection<? super E> collection) {
	return drainTo(collection, Integer.MAX_VALUE);
    }

    public int drainTo(Collection<? super E> collection, int maxElements) {
	Preconditions.checkNotNull(collection);
	if (collection == this)
	    throw new IllegalArgumentException();
	lockAll();
	try {
	    int i = 0;
	    for (; i < Math.min(count.get(), maxElements); i++) {
		count.decrementAndGet();
		collection.add(extract());
	    }
	    return i;
	} finally {
	    unlockAll();
	}
    }

    private void lockAll() {
	readLock.lock();
	writeLock.lock();
    }

    private void unlockAll() {
	readLock.lock();
	writeLock.lock();
    }

    /**
     * Returns a iterator for the data in the queue with the following
     * properties.
     * <p>
     * {@link CloseableIterator#removeData()} is an atomic operation.
     * <p>
     * Newer Objects added to different segment (other than the current) is not
     * seen.
     * <p>
     * You should call {@link CloseableIterator#close()} else files/segments
     * will not be un-referenced causing additional Disk space usage.
     * <p>
     * NOTE: you must call {@link CloseableIterator#close()} explicitly else the
     * additional disk space will be used until the iterator is GC'ed. This
     * requirement does not apply if the {@link CloseableIterator#hasNext()}
     * returns false.
     */
    public CloseableIterator<E> iterator() {
	return new ElementItrerator();
    }

    public class ElementItrerator extends AbstractIterator<E> implements
	    CloseableIterator<E> {
	private Queue<Segment<E>> allSegments;
	private int position;
	private SegmentEntry<E> current;

	private ElementItrerator() {
	    readLock.lock();
	    writeLock.lock();
	    try {
		allSegments = segments.cloneActive();
		position = allSegments.peek().getReadPosition();
	    } finally {
		readLock.unlock();
		writeLock.unlock();
	    }
	}

	@Override
	protected E computeNext() {
	    Segment<E> segment = null;
	    while ((segment = nextSegment()) != null) {
		if (!segment.hasData(position))
		    return null;
		SegmentEntry<E> element = null;
		if ((element = segment.readInternal(position)) != null) {
		    position += (element.size + Segment.ENTRY_OVERHEAD_SIZE);
		    current = element;
		    if (element.markDeleted)
			continue;
		    return element.element;
		}
	    }
	    endOfData();
	    close();
	    return null;
	}

	public Segment<E> nextSegment() {
	    Segment<E> segment = allSegments.peek();
	    if (!segment.hasData(position)) {
		Segment<E> seg = allSegments.poll();
		seg.referenced = false;
		position = 0;
	    }
	    return allSegments.peek();
	}

	public void removeData() {
	    Segment<E> seg = allSegments.poll();
	    seg.remove(position - current.size - Segment.ENTRY_OVERHEAD_SIZE);
	}

	public void close() {
	    if (current != null)
		current.parent.referenced = false;
	    Segment<E> segment;
	    while ((segment = allSegments.poll()) != null)
		segment.referenced = false;
	}

	protected void finalize() throws Throwable {
	    close();
	    super.finalize();
	}
    }
}
