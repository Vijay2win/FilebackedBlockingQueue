package com.win.queue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

public class MultiThreadedTest extends AbstractQueueTest
{
    private FileBackedBlockingQueue<Runnable> queue = new FileBackedBlockingQueue.Builder<Runnable>()
                                                            .directory(TEST_DIR)
                                                            .serializer(StringRunnable.serializer)
                                                            .build();
    private ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 10, 60L, TimeUnit.SECONDS, queue);;
    private static CountDownLatch latch = new CountDownLatch(2000);

    public static class StringRunnable implements Runnable
    {
        private String str;
        static QueueSerializer<Runnable> serializer = new StringSerializer();

        public StringRunnable(String str)
        {
            this.str = str;
        }

        public void run()
        {
            try
            {
                latch.countDown();
                Thread.sleep(1);
            }
            catch (InterruptedException e)
            {
            }
        }

        static class StringSerializer implements QueueSerializer<Runnable>
        {
            public byte[] serialize(Runnable t)
            {
                return ((StringRunnable) t).str.getBytes();
            }

            public Runnable deserialize(byte[] bytes)
            {
                // TODO Auto-generated method stub
                return new StringRunnable(new String(bytes));
            }

            public long serializedSize(Runnable t)
            {
                return ((StringRunnable) t).str.length();
            }
        }
    }

    @Test
    public void runAndAwait() throws InterruptedException
    {
        for (int i = 0; i < 2000; i++)
            executor.execute(new StringRunnable(TEST_STRING + i));
        latch.await();

        executor.shutdown();
        Assert.assertEquals(0, queue.size());
        Assert.assertTrue(executor.getActiveCount() + executor.getCompletedTaskCount() >= 2000);
    }

    @Test
    public void concurrentReadWrite() throws InterruptedException
    {
        final FileBackedBlockingQueue<String> queue = new FileBackedBlockingQueue.Builder<String>()
                .directory(TEST_DIR)
                .serializer(new StringSerializer())
                .build();
        final CountDownLatch latch = new CountDownLatch(2000);
        ExecutorService executor = Executors.newFixedThreadPool(10);
        for (int i = 0; i < 2000; i++)
        {
            executor.execute(new Runnable()
            {
                public void run()
                {
                    try
                    {
                        String str = queue.poll(Integer.MAX_VALUE, TimeUnit.SECONDS);
                        if (str != null)
                            latch.countDown();
                        Thread.sleep(1);
                    }
                    catch (InterruptedException e)
                    {
                        e.printStackTrace();
                    }
                }
            });
            queue.add(TEST_STRING);
        }
        latch.await();

        executor.shutdown();
        Assert.assertEquals(0, queue.size());
    }
}
