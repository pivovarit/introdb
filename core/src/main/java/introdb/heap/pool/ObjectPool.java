package introdb.heap.pool;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class ObjectPool<T> {

    private final ObjectFactory<T> fcty;
    private final ObjectValidator<T> validator;
    private final int maxPoolSize;

    private final ArrayBlockingQueue<T> freePool;
    private final ArrayBlockingQueue<CompletableFuture<T>> requests = new ArrayBlockingQueue<>(1024);

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
        if (validator.validate(object)) {
            var request = requests.poll();
            if (request != null) {
                request.complete(object);
            } else {
                freePool.offer(object);
            }
        } else {
            freePool.offer(fcty.create());
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
