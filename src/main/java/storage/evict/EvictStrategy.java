package storage.evict;

import storage.PageId;



public interface EvictStrategy {

    /**
     * 修改对应的数据结构以满足丢弃策略
     * @param pageId
     */
    void modifyData(PageId pageId);

    /**
     * 获取要丢弃的Page的PageId信息,用于丢弃
     * @return  PageId
     */
    PageId getEvictPageId();

}
