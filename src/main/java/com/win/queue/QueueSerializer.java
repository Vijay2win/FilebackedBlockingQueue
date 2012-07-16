package com.win.queue;

/**
 * Defines the method of serialization of the objects.
 * Advice: Never use Java's Serialization it is expensive.
 * 
 * @author Vijay Parthasarathy
 */
public interface QueueSerializer<T>
{
    /**
     * Serialize the specified type into the specified DataOutputStream
     * instance.
     */
    public byte[] serialize(T t);

    /**
     * Deserialize into the specified DataInputStream instance.
     */
    public T deserialize(byte[] bytes);

    /**
     * Calculate serialized size of object without actually serializing.
     */
    public long serializedSize(T t);
}
