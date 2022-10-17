package top.guoziyang.mydb.backend.dm.logger;

import com.google.common.primitives.Bytes;

import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.backend.utils.Parser;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 日志文件读写
 * 
 * 日志文件标准格式为：
 * [Log1] [Log2] ... [LogN]
 *
 * 每条正确日志的格式为：
 * [Size] [Data]
 * Size 4字节int 标识Data长度
 */
public class LoggerImpl implements Logger {
    private static final int OF_SIZE = 0;
    private static final int OF_DATA = OF_SIZE + 4;
    
    public static final String LOG_SUFFIX = ".log";

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock lock;

    private long position;  // 当前日志指针的位置
    private long fileSize;  // 初始化时记录，log操作不更新

    LoggerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        lock = new ReentrantLock();
    }


    void init() throws IOException {
        rewind();
        this.fileSize = file.length();
    }

    /**
     * 追加日志
     * @param data
     */
    @Override
    public void log(byte[] data) {
        lock.lock();
        try {
            fc.position(fc.size());
            fc.write(ByteBuffer.wrap(wrapLog(data)));
        } catch(IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
    }

    private byte[] wrapLog(byte[] data) {
        byte[] size = Parser.int2Byte(data.length);
        return Bytes.concat(size, data);
    }

    private byte[] internNext() {
        if(position + OF_DATA >= fileSize) {
            return null;
        }
        // 从当前position读取一个int
        ByteBuffer tmp = ByteBuffer.allocate(4);
        try {
            fc.position(position);
            fc.read(tmp);
        } catch(IOException e) {
            Panic.panic(e);
        }
        int size = Parser.parseInt(tmp.array());
        if(position + size + OF_DATA > fileSize) {
            return null;
        }

        ByteBuffer buf = ByteBuffer.allocate(OF_DATA + size);
        try {
            fc.position(position);
            fc.read(buf);
        } catch(IOException e) {
            Panic.panic(e);
        }

        byte[] log = buf.array();
        position += log.length;
        return log;
    }

    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            if(log == null) return null;
            return Arrays.copyOfRange(log, OF_DATA, log.length);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void rewind() {
        position = 4;
    }

    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        } catch(IOException e) {
            Panic.panic(e);
        }
    }
    
}
