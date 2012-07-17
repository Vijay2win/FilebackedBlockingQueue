FilebackedBlockingQueue
=======================

File based blocking queue is a simple java library utilizes MappedByteBuffer to store and retrieve data.
The functionality and complexity is comparable to LinkedBlockingQueue.

* The idea is most recent update to the Queue will be in the file cache and we will be accessed faster. 
* Bigger the file we will have to do a seek (sequential IO) to retrieve data in order.
* Removal of data from the queue is done by marking the data location and skipping them while reading.
* The filesystem/space is split into multiple segments. Each segments are created on demand.
* The segments are recycled (To reduce the number of FD's in use) after the data is retrieved.
* We also discard the segments instead of recycling if the max file system size is reached.
* Supports all the API's as LinkedBlockingQueue.

* Note: There is Serialization and de-serialization over, and it might be hard to do so with objects with state.
        Give care while implementing hashCode() and equals() methods for the elements of the queue.
        

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