package luva.xx.kafka.kjm.db;

import java.util.AbstractMap;
import java.util.Comparator;
import java.util.Set;

import org.rocksdb.RocksIterator;

/**
 * @author Hulva Luva.H
 * @date 2019年2月23日
 * @description
 *
 */
public class RocksDBRangeIterator extends RocksDbIterator {
	// RocksDB's JNI interface does not expose getters/setters that allow the
	// comparator to be pluggable, and the default is lexicographic, so it's
	// safe to just force lexicographic comparator here for now.
	private final Comparator<byte[]> comparator = Bytes.BYTES_LEXICO_COMPARATOR;
	private final byte[] rawToKey;

	RocksDBRangeIterator(final String storeName, final RocksIterator iter, final Set<KeyValueIterator<Bytes, byte[]>> openIterators,
			final Bytes from, final Bytes to) {
		super(storeName, iter, openIterators);
		iter.seek(from.get());
		rawToKey = to.get();
		if (rawToKey == null) {
			throw new NullPointerException("RocksDBRangeIterator: RawToKey is null for key " + to);
		}
	}

	@Override
	public AbstractMap.SimpleEntry<Bytes, byte[]> makeNext() {
		final AbstractMap.SimpleEntry<Bytes, byte[]> next = super.makeNext();

		if (next == null) {
			return allDone();
		} else {
			if (comparator.compare(next.getKey().get(), rawToKey) <= 0) {
				return next;
			} else {
				return allDone();
			}
		}
	}
}
