package com.win.queue;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Segment of the file system which is buffered.
 * 
 * @author Vijay Parthasarathy
 */
public class Segment<E> {
    private static final int END_OF_SEGMENT_MARKER = -1;
    static final int ENTRY_OVERHEAD_SIZE = 4 + 1;

    private final File logFile;
    private final RandomAccessFile logFileAccessor;
    private final MappedByteBuffer buffer;
    private final QueueSerializer<E> serializer;
    protected volatile boolean referenced = false;

    private int readPosition;

    protected Segment(File directory, long size, QueueSerializer<E> serializer) {
	this(directory, "Segment-" + System.nanoTime() + ".db", size,
		serializer);
    }

    private Segment(File directory, String fileName, long length,
	    QueueSerializer<E> serializer) {
	try {
	    if (length > Integer.MAX_VALUE)
		throw new IllegalArgumentException(
			"size > Integer.Max is not supported.");
	    this.serializer = serializer;
	    this.logFile = new File(directory, fileName);
	    logFileAccessor = new RandomAccessFile(logFile, "rw");
	    logFileAccessor.setLength(length);

	    buffer = logFileAccessor.getChannel().map(
		    FileChannel.MapMode.READ_WRITE, 0, length);
	    buffer.putInt(END_OF_SEGMENT_MARKER);
	} catch (IOException e) {
	    throw new IOError(e);
	}
    }

    void discard() {
	close();
	logFile.delete();
    }

    Segment<E> recycle() {
	buffer.position(0);
	buffer.putInt(END_OF_SEGMENT_MARKER);
	buffer.position(0);
	buffer.force();
	readPosition = 0;
	return this;
    }

    boolean hasCapacityFor(E element) {
	return (serializer.serializedSize(element) + ENTRY_OVERHEAD_SIZE) <= buffer
		.remaining();
    }

    void add(E element) {
	byte[] serializedRow = serializer.serialize(element);
	buffer.position(position());
	buffer.putInt(serializedRow.length);
	buffer.put((byte) 0);
	buffer.put(serializedRow);
	if (buffer.remaining() >= 4)
	    buffer.putInt(END_OF_SEGMENT_MARKER);
    }

    E read() {
	while (readPosition < position()) {
	    SegmentEntry<E> entry = readInternal(readPosition);
	    readPosition += (ENTRY_OVERHEAD_SIZE + entry.size);
	    if (entry.markDeleted)
		continue;
	    return entry.element;
	}
	return null;
    }

    E readWithoutSeek() {
	while (readPosition < position()) {
	    SegmentEntry<E> entry = readInternal(readPosition);
	    if (entry.markDeleted)
		continue;
	    return entry.element;
	}
	return null;
    }

    SegmentEntry<E> readInternal(int position) {
	ByteBuffer dupe = buffer.duplicate();
	dupe.position(position);
	int size = dupe.getInt();
	if (size == END_OF_SEGMENT_MARKER)
	    return null;
	byte b = dupe.get();
	if (b == 0) {
	    byte[] buffer = new byte[size];
	    dupe.get(buffer);
	    return new SegmentEntry<E>(this, size, false,
		    serializer.deserialize(buffer));
	}
	return new SegmentEntry<E>(this, size, true, null);
    }

    static class SegmentEntry<E> {
	final int size;
	final E element;
	final boolean markDeleted;
	final Segment<E> parent;

	public SegmentEntry(Segment<E> parent, int size, boolean markDeleted,
		E element) {
	    this.parent = parent;
	    this.size = size;
	    this.element = element;
	    this.markDeleted = markDeleted;
	}
    }

    String getName() {
	return logFile.getName();
    }

    void close() {
	try {
	    logFileAccessor.close();
	} catch (IOException e) {
	    throw new IOError(e);
	}
    }

    @Override
    public String toString() {
	return "Segment(" + getName() + ')';
    }

    int position() {
	return buffer.position() - 4;
    }

    boolean hasData() {
	return hasData(readPosition);
    }

    boolean hasData(int position) {
	return position < position();
    }

    public int getReadPosition() {
	return readPosition;
    }

    public void remove(int position) {
	ByteBuffer dupe = buffer.duplicate();
	dupe.position(position + 4);
	dupe.put((byte) -1);
    }
}
