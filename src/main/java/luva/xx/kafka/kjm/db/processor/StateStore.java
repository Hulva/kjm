package luva.xx.kafka.kjm.db.processor;

/**
 * @author Hulva Luva.H
 * @date 2019年2月23日
 * @description
 *
 */
public interface StateStore {
	/**
	 * The name of this store.
	 * 
	 * @return the storage name
	 */
	String name();

	/**
	 * Flush any cached data
	 */
	void flush();

	/**
	 * Close the storage engine. Note that this function needs to be idempotent
	 * since it may be called several times on the same state store.
	 * <p>
	 * Users only need to implement this function but should NEVER need to call this
	 * api explicitly as it will be called by the library automatically when
	 * necessary
	 */
	void close();

	/**
	 * Return if the storage is persistent or not.
	 *
	 * @return {@code true} if the storage is persistent&mdash;{@code false}
	 *         otherwise
	 */
	boolean persistent();

	/**
	 * Is this store open for reading and writing
	 * 
	 * @return {@code true} if the store is open
	 */
	boolean isOpen();
}
