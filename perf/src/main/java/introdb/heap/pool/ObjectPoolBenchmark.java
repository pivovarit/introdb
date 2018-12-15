package introdb.heap.pool;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantLock;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Benchmark)
public class ObjectPoolBenchmark {

	private ObjectPool<ReentrantLock> pool;

	@Setup(Level.Iteration)
	public void setUp() throws IOException, ClassNotFoundException {
		pool = new ObjectPool<>(ReentrantLock::new, (ReentrantLock l) -> !l.isLocked());
	}

	@TearDown(Level.Iteration)
	public void tearDown() throws IOException, InterruptedException {
		pool.shutdown();
	}

	@Benchmark
	@Threads(8)
	public void testPool_8_threads(Blackhole blackhole) throws InterruptedException, ExecutionException {
		CompletableFuture<ReentrantLock> future = pool.borrowObject();
		ReentrantLock lock = future.get();
		try {
			blackhole.consume(lock);
		} finally {
			pool.returnObject(lock);
		}
	}

	@Benchmark
	@Threads(1)
	public void testPool_1_thread(Blackhole blackhole) throws InterruptedException, ExecutionException {
		CompletableFuture<ReentrantLock> future = pool.borrowObject();
		ReentrantLock lock = future.get();
		try {
			blackhole.consume(lock);
		} finally {
			pool.returnObject(lock);
		}
	}

	public static void main(String[] args) throws RunnerException {
		var opt = new OptionsBuilder()
		.include(ObjectPoolBenchmark.class.getSimpleName())
		.warmupIterations(5)
		.measurementIterations(5)
		.forks(1)
		.build();

		var result = new Runner(opt).run();
	}

}
