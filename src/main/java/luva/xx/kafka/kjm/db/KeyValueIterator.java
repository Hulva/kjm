package luva.xx.kafka.kjm.db;

import java.io.Closeable;
import java.util.AbstractMap;
import java.util.Iterator;

/**
 * @author Hulva Luva.H
 * @date 2019年2月23日
 * @description
 *
 */
public interface KeyValueIterator<K, V> extends Iterator<AbstractMap.SimpleEntry<K, V>>, Closeable {
	@Override
	void close();

	/**
	 * Peek at the next key without advancing the iterator
	 * 
	 * @return the key of the next value that would be returned from the next call
	 *         to next
	 */
	K peekNextKey();
}
