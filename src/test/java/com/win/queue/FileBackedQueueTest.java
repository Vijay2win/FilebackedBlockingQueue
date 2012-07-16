package com.win.queue;

import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Test;

public class FileBackedQueueTest extends AbstractQueueTest
{
    @Test
    public void tesOffer()
    {
        FileBackedBlockingQueue<String> queue = new FileBackedBlockingQueue.Builder<String>().directory(TEST_DIR).serializer(new StringSerializer())
                .segmentSize((TEST_STRING.length() + Segment.ENTRY_OVERHEAD_SIZE + 2) * 100).build();
        for (int i = 0; i < 1024; i++)
            queue.offer(TEST_STRING);
        Assert.assertTrue(TEST_DIR.list().length > 0);
    }

    @Test
    public void testpoll()
    {
        // create a queue to hold the test data.
        FileBackedBlockingQueue<String> queue = new FileBackedBlockingQueue.Builder<String>().directory(TEST_DIR).serializer(new StringSerializer())
                .segmentSize((TEST_STRING.length() + Segment.ENTRY_OVERHEAD_SIZE) * 100).build();

        // init
        for (int i = 0; i < 2000; i++)
            queue.add(TEST_STRING);
        for (int i = 0; i < 2000; i++)
        {
            if (i <= 1000)
                Assert.assertTrue(queue.segments.getActiveSegments() > 10);
            else
                Assert.assertTrue("size : " + queue.segments.getInActiveSegments(), queue.segments.getInActiveSegments() >= 9);
            Assert.assertEquals(TEST_STRING, queue.poll());
        }
    }

    @Test
    public void testInactives()
    {
        FileBackedBlockingQueue<String> queue = new FileBackedBlockingQueue.Builder<String>().directory(TEST_DIR).serializer(new StringSerializer())
                .segmentSize((TEST_STRING.length() + Segment.ENTRY_OVERHEAD_SIZE + 2) * 100).build();
        // add more to see if we get the same names in order.
        for (int i = 0; i < 1000; i++)
            queue.add(TEST_STRING + i);
        for (int i = 0; i < 1000; i++)
        {
            if (i > 100)
                Assert.assertTrue(queue.segments.getInActiveSegments() >= 0);
            Assert.assertEquals(TEST_STRING + i, queue.poll());
        }
    }

    @Test
    public void testPeek()
    {
        // mark the segment size approx for 100 elements.
        FileBackedBlockingQueue<String> queue = new FileBackedBlockingQueue.Builder<String>().directory(TEST_DIR).serializer(new StringSerializer())
                .segmentSize((TEST_STRING.length() + Segment.ENTRY_OVERHEAD_SIZE + 10) * 100).build();
        for (int i = 0; i < 2000; i++)
            queue.add(TEST_STRING + i);
        for (int i = 0; i < 2000; i++)
            Assert.assertEquals(queue.peek(), queue.poll());
    }

    @Test
    public void testDrain()
    {
        // mark the segment size approx for 100 elements.
        FileBackedBlockingQueue<String> queue = new FileBackedBlockingQueue.Builder<String>().directory(TEST_DIR).serializer(new StringSerializer())
                .segmentSize((TEST_STRING.length() + Segment.ENTRY_OVERHEAD_SIZE + 10) * 100).build();
        // init
        for (int i = 0; i < 2000; i++)
            queue.add(TEST_STRING + i);
        queue.drainTo(new ArrayList<String>(), 1000);

        for (int i = 1000; i < 2000; i++)
            Assert.assertEquals(TEST_STRING + i, queue.poll());
    }
}
