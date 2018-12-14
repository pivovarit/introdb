package introdb.heap.pool;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class ObjectPool<T> {

    private ObjectFactory<T> fcty;
    private final ObjectValidator<T> validator;
    private final int maxPoolSize;

    private final ArrayBlockingQueue<T> freePool;
    private final AtomicInteger nextIndex = new AtomicInteger(0);

    public ObjectPool(ObjectFactory<T> fcty, ObjectValidator<T> validator) {
        this(fcty, validator, 25);
    }

    public ObjectPool(ObjectFactory<T> fcty, ObjectValidator<T> validator, int maxPoolSize) {
        this.fcty = fcty;
        this.validator = validator;
        this.maxPoolSize = maxPoolSize;
        this.freePool = new ArrayBlockingQueue<>(maxPoolSize);
    }

    public CompletableFuture<T> borrowObject() {
        T obj;
        if (null != (obj = freePool.poll())) { // if there's a free object, return
            return completedFuture(obj);
        }

        if (nextIndex.get() == maxPoolSize) { // if pool saturated, wait async
            return spinWaitAsync();
        }

        // spawn new object
        int claimed;
        int next;
        do {
            claimed = nextIndex.get();
            next = claimed + 1;
            if (next > maxPoolSize) { // when competing thread reached max first, wait
                return spinWaitAsync();
            }
        } while (!nextIndex.compareAndSet(claimed, next));

        T object = fcty.create();
        if (next == maxPoolSize) { // if pool initialized, factory not needed
            fcty = null;
        }

        return completedFuture(object);
    }

    public void returnObject(T object) {
        if (!validator.validate(object)) {
            throw new IllegalStateException("Object is still in use!");
        }
        freePool.offer(object);
    }

    public void shutdown() throws InterruptedException {
    }

    public int getPoolSize() {
        return nextIndex.get();
    }

    public int getInUse() {
        return nextIndex.get() - freePool.size();
    }

    private CompletableFuture<T> spinWaitAsync() {
        return CompletableFuture.supplyAsync(() -> {
            T obj;

            while (null == (obj = freePool.poll())) {
                Thread.onSpinWait();
            }

            return obj;
        });
    }
}
