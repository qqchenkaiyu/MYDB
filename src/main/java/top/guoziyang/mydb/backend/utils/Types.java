package top.guoziyang.mydb.backend.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class Types {
    public static long addressToUid(int pgno, short offset) {
        return pgno << 32 | offset;
    }

    public static long readLong(FileChannel fc) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(8);
        fc.read(buf);
        return buf.getLong(0);
    }
    public static void writeLong(FileChannel fc,long value) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(8);
        buf.putLong(value).rewind();
        fc.write(buf);
    }

    public static byte readByte(FileChannel fc) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(1);
        fc.read(buf);
        return buf.get(0);
    }

    public static void writeByte(FileChannel fc,byte value) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(1);
        buf.put(value).rewind();
        fc.write(buf);
    }
}
