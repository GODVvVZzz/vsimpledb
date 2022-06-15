package storage.evict;

import storage.PageId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @description:    LRU页面丢弃策略
 * 根据历史访问记录来淘汰数据，核心思想为；如果数据最近被访问过，那么将来被访问的几率页更高
 */

public class LRUEvict implements EvictStrategy {

    private DLinkedNode head, tail;
    private Map<PageId, DLinkedNode> map;

    public LRUEvict(int numPages) {
        head = new DLinkedNode();
        tail = new DLinkedNode();
        head.next = tail;
        tail.prev = head;
        map = new ConcurrentHashMap<>(numPages);
    }

    @Override
    public void modifyData(PageId pageId) {
        if (map.containsKey(pageId)) {
            DLinkedNode node = map.get(pageId);
            moveToHead(node);
        } else {
            DLinkedNode node = new DLinkedNode(pageId);
            map.put(pageId, node);
            addToHead(node);
        }
    }

    @Override
    public PageId getEvictPageId() {
        return removeTail().getValue();
    }

    private void addToHead(DLinkedNode node) {
        node.prev = head;
        node.next = head.next;
        head.next.prev = node;
        head.next = node;
    }

    private void removeNode(DLinkedNode node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
        map.remove(node.value);
    }

    private void moveToHead(DLinkedNode node) {
        removeNode(node);
        addToHead(node);
    }

    private DLinkedNode removeTail() {
        DLinkedNode res = tail.prev;
        removeNode(res);
        return res;
    }
}
