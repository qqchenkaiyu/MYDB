package top.guoziyang.mydb.backend.dm.logger;

import cn.hutool.core.io.FileUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public interface Logger {
    void log(byte[] data);
    byte[] next();
    void rewind();
    void close();

    static Logger create(String path) throws IOException {
        File f = FileUtil.file(path + LoggerImpl.LOG_SUFFIX);
        if (f.exists()) {
            f.delete();
        }
        f.createNewFile();
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        FileChannel fc = raf.getChannel();
        // 初始化写入空
        fc.write(ByteBuffer.allocate(4));
        fc.force(false);
        return new LoggerImpl(raf, fc);
    }

    static Logger open(String path) throws IOException {
        File f=FileUtil.file(path+LoggerImpl.LOG_SUFFIX);
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        FileChannel fc = raf.getChannel();
        LoggerImpl lg = new LoggerImpl(raf, fc);
        lg.init();
        return lg;
    }
}
