package introdb.heap.pool;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class ObjectPool<T> {

    private ObjectFactory<T> fcty;
    private final ObjectValidator<T> validator;
    private final int maxPoolSize;

    private final ConcurrentLinkedQueue<T> freePool = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<CompletableFuture<T>> requests = new ConcurrentLinkedQueue<>();

    private final AtomicInteger nextIndex = new AtomicInteger(0);

    public ObjectPool(ObjectFactory<T> fcty, ObjectValidator<T> validator) {
        this(fcty, validator, 25);
    }

    public ObjectPool(ObjectFactory<T> fcty, ObjectValidator<T> validator, int maxPoolSize) {
        this.fcty = fcty;
        this.validator = validator;
        this.maxPoolSize = maxPoolSize;
    }

    public CompletableFuture<T> borrowObject() {
        T obj;
        if (null != (obj = freePool.poll())) { // if there's a free object waiting, return
            return completedFuture(obj);
        } else if (nextIndex.get() == maxPoolSize) { // if pool saturated, return in
            return uncompletedRequest();
        } else { // spawn new object
            int claimed;
            int next;
            do {
                claimed = nextIndex.get();
                next = claimed + 1;
                if (next > maxPoolSize) { // when competing thread reached max first, wait
                    return uncompletedRequest();
                }
            } while (!nextIndex.compareAndSet(claimed, next));

            T object = fcty.create();
            if (next == maxPoolSize) { // if pool initialized, factory not needed
                fcty = null;
            }

            return completedFuture(object);
        }
    }

    public void returnObject(T object) {
        if (!validator.validate(object)) {
            throw new IllegalStateException("Object is still in use!");
        }
        var request = requests.poll();
        if (request != null) {
            request.complete(object);
        } else {
            freePool.offer(object);
        }
    }

    public void shutdown() throws InterruptedException {
    }

    public int getPoolSize() {
        return nextIndex.get();
    }

    public int getInUse() {
        return nextIndex.get() - freePool.size();
    }

    private CompletableFuture<T> uncompletedRequest() {
        var req = new CompletableFuture<T>();
        requests.add(req);
        return req;
    }
}
