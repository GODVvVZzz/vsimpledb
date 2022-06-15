package storage;

import common.Database;
import common.DbException;
import common.Permissions;
import storage.evict.EvictStrategy;
import storage.evict.LRUEvict;
import storage.lock.LockManager;
import transaction.TransactionAbortedException;
import transaction.TransactionId;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author HP
 * @date 2022/5/21
 */
public class BufferPool {

    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    public static final int DEFAULT_PAGES = 50;

    private Integer numPages;
    private Map<PageId, Page> pageCache;
    private EvictStrategy evict;
    private LockManager lockManager;

    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        this.pageCache = new ConcurrentHashMap<>();
        this.evict = new LRUEvict(numPages);
        this.lockManager = new LockManager();
    }

    public static int getPageSize() {
        return pageSize;
    }

    public synchronized Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        int acquireType = 0;
        if (perm == Permissions.READ_WRITE) {
            acquireType = 1;
        }
        long start = System.currentTimeMillis();
        long timeout = new Random().nextInt(2000) + 1000;
        while (true) {
            try {
                if (lockManager.acquireLock(pid, tid, acquireType)) {
                    break;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            long now = System.currentTimeMillis();
            if (now - start > timeout) {
                throw new TransactionAbortedException();
            }
        }
        if (!pageCache.containsKey(pid)) {
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = dbFile.readPage(pid);
            evict.modifyData(pid);
            if (pageCache.size() == numPages) {
                evictPage();
            }
            pageCache.put(pid, page);
        }
        return pageCache.get(pid);
    }


    public void transactionComplete(TransactionId tid, boolean commit) {
        if (commit) {
            try {
                flushPages(tid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            // 从磁盘重新加载脏页
            recoverPages(tid);
        }
        lockManager.completeTransaction(tid);
    }

    private synchronized void recoverPages(TransactionId tid) {
        for (Map.Entry<PageId, Page> entry : pageCache.entrySet()) {
            PageId pid = entry.getKey();
            Page page = entry.getValue();
            if (page.isDirty() == tid) {
                int tableId = pid.getTableId();
                DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
                Page cleanPage = dbFile.readPage(pid);
                pageCache.put(pid, cleanPage);
            }
        }
    }

    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        updateBufferPool(dbFile.insertTuple(tid, t), tid);
        System.out.println("insert 1 tuple success");
    }

    public  void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        DbFile dbFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        updateBufferPool(dbFile.deleteTuple(tid, t), tid);
    }

    private void updateBufferPool(List<Page> pages, TransactionId tid) throws DbException {
        for (Page page : pages) {
            page.markDirty(true, tid);
            if (pageCache.size() == numPages) {
                evictPage();
            }
            pageCache.put(page.getId(), page);
        }
    }


    public synchronized void discardPage(PageId pid) {

        pageCache.remove(pid);
    }

    private synchronized  void flushPage(PageId pid) throws IOException {

        Page flush = pageCache.get(pid);

        // 通过tableId找到对应的DbFile,并将page写入到对应的DbFile中
        int tableId = pid.getTableId();
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);

        // 将page刷新到磁盘
        dbFile.writePage(flush);
        flush.markDirty(false, null);
    }

    public synchronized  void flushPages(TransactionId tid) throws IOException {

        for (Map.Entry<PageId, Page> entry : pageCache.entrySet()) {
            Page page = entry.getValue();
            if (page.isDirty() == tid) {
                flushPage(page.getId());
            }
        }
    }

    private synchronized  void evictPage() throws DbException {

        PageId evictPageId = null;
        Page page = null;
        boolean isAllDirty = true;
        for (int i = 0; i < pageCache.size(); i++) {
            evictPageId = evict.getEvictPageId();
            page = pageCache.get(evictPageId);
            if (page.isDirty() != null) {
                evict.modifyData(evictPageId);
            } else {
                isAllDirty = false;
                discardPage(evictPageId);
                pageCache.remove(evictPageId);
                break;
            }
        }
        if (isAllDirty) {
            throw new DbException("缓冲池中全为脏页");
        }
    }
}
