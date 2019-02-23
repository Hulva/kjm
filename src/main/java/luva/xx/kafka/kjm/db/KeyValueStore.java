package luva.xx.kafka.kjm.db;

import java.util.AbstractMap;
import java.util.List;

import luva.xx.kafka.kjm.db.processor.StateStore;

/**
 * @author Hulva Luva.H
 * @date 2019年2月23日
 * @description
 *
 */
public interface KeyValueStore<K, V> extends StateStore, ReadOnlyKeyValueStore<K, V> {

	/**
	 * Update the value associated with this key.
	 *
	 * @param key   The key to associate the value to
	 * @param value The value to update, it can be {@code null}; if the serialized
	 *              bytes are also {@code null} it is interpreted as deletes
	 * @throws NullPointerException If {@code null} is used for key.
	 */
	void put(K key, V value);

	/**
	 * Update the value associated with this key, unless a value is already
	 * associated with the key.
	 *
	 * @param key   The key to associate the value to
	 * @param value The value to update, it can be {@code null}; if the serialized
	 *              bytes are also {@code null} it is interpreted as deletes
	 * @return The old value or {@code null} if there is no such key.
	 * @throws NullPointerException If {@code null} is used for key.
	 */
	V putIfAbsent(K key, V value);

	/**
	 * Update all the given key/value pairs.
	 *
	 * @param entries A list of entries to put into the store; if the serialized
	 *                bytes are also {@code null} it is interpreted as deletes
	 * @throws NullPointerException If {@code null} is used for key.
	 */
	void putAll(List<AbstractMap.SimpleEntry<K, V>> entries);

	/**
	 * Delete the value from the store (if there is one).
	 *
	 * @param key The key
	 * @return The old value or {@code null} if there is no such key.
	 * @throws NullPointerException If {@code null} is used for key.
	 */
	V delete(K key);
}
