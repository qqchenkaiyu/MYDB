package top.guoziyang.mydb.backend.common;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.util.Collection;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * AbstractCache 实现了一个引用计数策略的缓存
 */
public abstract class AbstractCache<T> {
                            // 缓存中元素的个数
    private Lock lock;
    private Cache<Long, T> loadingCache;
    public AbstractCache() {
        lock = new ReentrantLock();
        //创建guava cache
        loadingCache = Caffeine.newBuilder()
            .build();
    }

    protected T get(long key) {
        return loadingCache.get(key, s-> {
            try {
                return getForCache(s);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        });
    }

    /**
     * 强行释放一个缓存
     */
    protected void release(long key) {
        releaseForCache(get(key));
    }

    /**
     * 关闭缓存，写回所有资源
     */
    protected void close() {
        lock.lock();
        try {
            Collection<T> values = loadingCache.asMap().values();
            for (T value : values) {
                releaseForCache(value);
            }
        } finally {
            lock.unlock();
        }
    }


    /**
     * 当资源不在缓存时的获取行为
     */
    protected abstract T getForCache(long key) throws Exception;
    /**
     * 当资源被驱逐时的写回行为
     */
    protected abstract void releaseForCache(T obj);
}
