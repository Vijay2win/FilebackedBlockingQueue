FilebackedBlockingQueue
=======================

File based blocking queue is a simple java library, utilizes MappedByteBuffer to store and retrieve data.
The functionality and complexity is comparable to LinkedBlockingQueue.

* Most recent update to the Queue will be in the file cache and hence an immediate read will be faster. 
* Bigger the queue, we will have to do a seek (but sequential IO) to retrieve data in order.
* Removing am element is same as marking the data location for delete and skipping them while reading. (Hence atomic)
* The filesystem/space is split into multiple segments. Each segment is created on demand as the queue grows.
* The segments are recycled (To reduce the number of FD's in use) after the data is retrieved.
* Segments are discard instead of recycling if the max file system size has reached.
* Supports BlockingQueue API's.

* Note: There is Serialization and de-serialization overhead. It might be hard to serialize objects with states.

Example Code:
=============

        FileBackedBlockingQueue<String> queue = new FileBackedBlockingQueue.Builder<String>().directory(TEST_DIR).serializer(new StringSerializer()).build();
        for (int i = 0; i < 2000; i++)
            queue.add(TEST_STRING + i);

        (OR)

        FileBackedBlockingQueue<Runnable> workQueue = new FileBackedBlockingQueue.Builder<Runnable>().directory(dir).serializer(MessageTask.serializer).build();
        executor = new ThreadPoolExecutor(threadCount, threadCount, 60L, TimeUnit.SECONDS, workQueue);
        if (pattern.matcher(msgContainer.routingKey).matches())
            executor.execute(new MessageTask(msgContainer));