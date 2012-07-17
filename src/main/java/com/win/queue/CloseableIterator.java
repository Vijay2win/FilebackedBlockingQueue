package com.win.queue;

import java.util.Iterator;

/**
 * Extends Iterator interface to add {@link #close()}
 * 
 * @author Vijay Parthasarathy
 */
public interface CloseableIterator<T> extends Iterator<T>
{
    public void removeData();
    public void close();
}
