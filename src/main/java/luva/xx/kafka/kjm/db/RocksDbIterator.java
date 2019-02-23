package luva.xx.kafka.kjm.db;

import java.util.AbstractMap;
import java.util.NoSuchElementException;
import java.util.Set;

import org.rocksdb.RocksIterator;

/**
 * @author Hulva Luva.H
 * @date 2019年2月23日
 * @description
 *
 */
public class RocksDbIterator extends AbstractIterator<AbstractMap.SimpleEntry<Bytes, byte[]>> implements KeyValueIterator<Bytes, byte[]> {
	private final String storeName;
	private final RocksIterator iter;
	private final Set<KeyValueIterator<Bytes, byte[]>> openIterators;

	private volatile boolean open = true;

	private AbstractMap.SimpleEntry<Bytes, byte[]> next;

	RocksDbIterator(final String storeName, final RocksIterator iter, final Set<KeyValueIterator<Bytes, byte[]>> openIterators) {
		this.storeName = storeName;
		this.iter = iter;
		this.openIterators = openIterators;
	}

	@Override
	public synchronized boolean hasNext() {
		if (!open) {
			throw new RuntimeException(String.format("InvalidStateStoreException: RocksDB iterator for store %s has closed", storeName));
		}
		return super.hasNext();
	}

	@Override
	public AbstractMap.SimpleEntry<Bytes, byte[]> makeNext() {
		if (!iter.isValid()) {
			return allDone();
		} else {
			next = getKeyValue();
			iter.next();
			return next;
		}
	}

	private AbstractMap.SimpleEntry<Bytes, byte[]> getKeyValue() {
		return new AbstractMap.SimpleEntry<>(new Bytes(iter.key()), iter.value());
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("RocksDB iterator does not support remove()");
	}

	@Override
	public synchronized void close() {
		openIterators.remove(this);
		iter.close();
		open = false;
	}

	@Override
	public Bytes peekNextKey() {
		if (!hasNext()) {
			throw new NoSuchElementException();
		}
		return next.getKey();
	}
}
