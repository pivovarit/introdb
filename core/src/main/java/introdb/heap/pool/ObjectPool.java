package introdb.heap.pool;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.CompletableFuture.completedFuture;

/*
    2.9 GHz Intel Core i7
        Total Number of Cores:	4
        L2 Cache (per Core):	256 KB
        L3 Cache:	8 MB
    16 GB 2133 MHz LPDDR3
    APPLE SSD SM0512L

    Benchmark                                 Mode  Cnt         Score        Error  Units
    ObjectPoolBenchmark.testPool_8_threads   thrpt    5  10130575.306 ? 708935.411  ops/s
    ObjectPoolBenchmark.testPool_1_thread    thrpt    5  19462125.400 ? 331034.302  ops/s

 */
public class ObjectPool<T> {

    private final ObjectFactory<T> fcty;
    private final ObjectValidator<T> validator;
    private final int maxPoolSize;

    private final ArrayBlockingQueue<T> freePool;
    private final ConcurrentLinkedQueue<CompletableFuture<T>> requests = new ConcurrentLinkedQueue<>();

    private final AtomicInteger poolSize = new AtomicInteger(0);

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
        if (null != (obj = freePool.poll())) { // if there's a free object waiting, return
            return completedFuture(obj);
        } else if (poolSize.get() == maxPoolSize) { // if pool saturated, return in
            return uncompletedRequest();
        } else { // spawn new object
            int claimed;
            int next;
            do {
                claimed = poolSize.get();
                next = claimed + 1;
                if (next > maxPoolSize) { // when competing thread reached max first, wait
                    return uncompletedRequest();
                }
            } while (!poolSize.compareAndSet(claimed, next));

            T object = fcty.create();

            return completedFuture(object);
        }
    }

    public void returnObject(T object) {
        T objectToAdd = validator.validate(object) ? object : fcty.create();
        var request = requests.poll();
        if (request != null) {
            request.complete(objectToAdd);
        } else {
            freePool.offer(objectToAdd);
        }
    }

    public void shutdown() throws InterruptedException {
    }

    public int getPoolSize() {
        return poolSize.get();
    }

    public int getInUse() {
        return poolSize.get() - freePool.size();
    }

    private CompletableFuture<T> uncompletedRequest() {
        var req = new CompletableFuture<T>();
        requests.add(req);
        return req;
    }
}
