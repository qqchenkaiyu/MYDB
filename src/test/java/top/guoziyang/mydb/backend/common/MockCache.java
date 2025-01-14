package top.guoziyang.mydb.backend.common;

public class MockCache extends AbstractCache<Long> {

    public MockCache() {
    }

    @Override
    protected Long getForCache(long key) throws Exception {
        return key;
    }

    @Override
    protected void releaseForCache(Long obj) {}
    
}
