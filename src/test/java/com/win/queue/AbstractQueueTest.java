package com.win.queue;

import java.io.File;

import org.junit.AfterClass;
import org.junit.BeforeClass;

public class AbstractQueueTest {
    static File TEST_DIR = new File("target/FileQueueTest");;
    static String TEST_STRING = "This is tested perfectly by Vijay";

    public class StringSerializer implements QueueSerializer<String> {
	public byte[] serialize(String t) {
	    return t.getBytes();
	}

	public String deserialize(byte[] bytes) {
	    return new String(bytes);
	}

	public long serializedSize(String t) {
	    return t.length();
	}
    }

    @BeforeClass
    public static void setup() {
	if (TEST_DIR.exists())
	    for (File file : TEST_DIR.listFiles())
		file.delete();
	TEST_DIR.mkdirs();
    }

    @AfterClass
    public static void clear() {
	TEST_DIR.delete();
    }
}
