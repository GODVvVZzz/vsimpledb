package common;

import storage.BufferPool;

import java.util.concurrent.atomic.AtomicReference;

/**
 * @author HP
 * @date 2022/5/21
 */
public class Database {

    private static final AtomicReference<Database> _instance = new AtomicReference<>(new Database());
    private final Catalog _catalog;
    private final BufferPool _bufferpool;

    private Database() {
        _catalog = new Catalog();
        _bufferpool = new BufferPool(BufferPool.DEFAULT_PAGES);
    }

    public static BufferPool getBufferPool() {
        return _instance.get()._bufferpool;
    }

    public static Catalog getCatalog() {
        return _instance.get()._catalog;
    }

    public static void reset() {
        _instance.set(new Database());
    }
}
