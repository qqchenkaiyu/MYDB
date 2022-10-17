package top.guoziyang.mydb.backend.dm.pageIndex;

import top.guoziyang.mydb.backend.dm.pageCache.PageCache;

import java.util.concurrent.ConcurrentLinkedDeque;

public class PageIndex {
    // 分为40个区间 409b的都在第一个区间 818b的都在第二个区间
    private static final int INTERVALS_NO = 40;
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    private ConcurrentLinkedDeque<PageInfo>[] lists;



    @SuppressWarnings("unchecked")
    public PageIndex() {
        lists = new ConcurrentLinkedDeque[INTERVALS_NO+1];
        for (int i = 0; i < INTERVALS_NO+1; i ++) {
            lists[i] = new ConcurrentLinkedDeque<>();
        }
    }

    public PageInfo add(int pgno, int freeSpace) {
        int number = freeSpace / THRESHOLD;
        PageInfo pageInfo = new PageInfo(pgno, freeSpace);
        lists[number].add(pageInfo);
        return pageInfo;
    }

    public PageInfo select(int spaceSize) {
            int number = spaceSize / THRESHOLD;
            if(number < INTERVALS_NO) number ++;
            while(number <= INTERVALS_NO) {
                if(lists[number].size() == 0) {
                    number ++;
                    continue;
                }
                return lists[number].removeFirst();
            }
            return null;
    }

}
