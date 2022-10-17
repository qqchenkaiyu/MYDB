package top.guoziyang.mydb.backend.dm.pageCache;

import cn.hutool.core.io.FileUtil;
import top.guoziyang.mydb.backend.dm.page.Page;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
// 这里的缓存 就可以用弱一级 的 软引用
public interface PageCache {
    
    public static final int PAGE_SIZE = 1 << 13;

    Page newPage(byte[] initData);
    Page getPage(int pgno) throws Exception;
    void close();
    void release(Page page);

    void truncateByBgno(int maxPgno);
    int getPageNumber();
    void flushPage(Page pg);

     static PageCacheImpl open(String path) throws IOException {
        File f = FileUtil.file(path+PageCacheImpl.DB_SUFFIX);
        if (!f.exists()) {
            f.createNewFile();
        }
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        FileChannel fc = raf.getChannel();
        return new PageCacheImpl(raf, fc);
    }
}
