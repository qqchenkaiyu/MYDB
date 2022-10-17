package top.guoziyang.mydb.backend.tm;

import cn.hutool.core.io.FileUtil;

import static top.guoziyang.mydb.backend.utils.Types.writeLong;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public interface TransactionManager {
    long begin() throws IOException;
    void commit(long xid) throws IOException;
    void abort(long xid) throws IOException;
    boolean isActive(long xid) throws IOException;
    boolean isCommitted(long xid) throws IOException;
    boolean isAborted(long xid) throws IOException;
    void close();

    static TransactionManagerImpl create(String path) throws IOException {
        File f = FileUtil.file(path + TransactionManagerImpl.XID_SUFFIX);
        f.createNewFile();
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        FileChannel fc = raf.getChannel();
        // 写空XID文件头
        writeLong(fc,0);
        return new TransactionManagerImpl(raf, fc);
    }

    static TransactionManagerImpl open(String path) throws IOException {
        File f = FileUtil.file(path + TransactionManagerImpl.XID_SUFFIX);
        f.createNewFile();
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        FileChannel fc = raf.getChannel();
        return new TransactionManagerImpl(raf, fc);
    }
}
