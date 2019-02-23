package luva.xx.kafka.kjm.test.rocksdb;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Test;
import org.rocksdb.RocksDBException;

import luva.xx.kafka.kjm.db.Bytes;
import luva.xx.kafka.kjm.db.RocksDBStore;

/**
 * @author Hulva Luva.H
 * @date 2019年2月23日
 * @description
 *
 */
public class TestRocksDB {

	@Test
	public void test() throws RocksDBException {
//		RocksDBStore.build();
//		for (int i = 500000; i < 5000000; i++) {
//			Bytes bytes = new Bytes(("Hello" + i).getBytes());
//			RocksDBStore.build().put(bytes, ("World@xx-" + i).getBytes());
//			System.out.println(new String(RocksDBStore.build().get(bytes)));
//		}
//		RocksDBStore.build().getDB().compactRange();
		System.out.println(RocksDBStore.build().approximateNumEntries());
//		RocksDBStore.build().all().forEachRemaining(action -> {
//			System.out.println(action.getKey() + " -> " + new String(action.getValue()));
//			RocksDBStore.build().delete(action.getKey());
//		});
//		Bytes from = new Bytes(("Hello").getBytes());
//		Bytes to = new Bytes(("Hello" + 2).getBytes());
//		final AtomicInteger counter = new AtomicInteger();
//		RocksDBStore.build().range(from, to).forEachRemaining(action -> {
//			System.out.println(counter.incrementAndGet() + ": " + action.getKey() + " -> " + new String(action.getValue()));
//		});
	}

	@After
	public void release() {
		RocksDBStore.build().close();
	}

}
