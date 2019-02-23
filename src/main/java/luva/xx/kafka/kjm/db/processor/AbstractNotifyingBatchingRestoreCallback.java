package luva.xx.kafka.kjm.db.processor;

/**
 * @author Hulva Luva.H
 * @date 2019年2月23日
 * @description
 *
 */
public abstract class AbstractNotifyingBatchingRestoreCallback<T> implements BatchingStateRestoreCallback, StateRestoreListener<T> {

	/**
	 * Single put restore operations not supported, please use
	 * {@link AbstractNotifyingRestoreCallback} or {@link StateRestoreCallback}
	 * instead for single action restores.
	 */
	@Override
	public void restore(final byte[] key, final byte[] value) {
		throw new UnsupportedOperationException("Single restore not supported");
	}

	/**
	 * @see StateRestoreListener#onRestoreStart(TopicPartition, String, long, long)
	 *
	 *      This method does nothing by default; if desired, subclasses should
	 *      override it with custom functionality.
	 *
	 */
	@Override
	public void onRestoreStart(final T topicPartition, final String storeName, final long startingOffset, final long endingOffset) {

	}

	/**
	 * @see StateRestoreListener#onBatchRestored(TopicPartition, String, long, long)
	 *
	 *      This method does nothing by default; if desired, subclasses should
	 *      override it with custom functionality.
	 *
	 */
	@Override
	public void onBatchRestored(final T topicPartition, final String storeName, final long batchEndOffset, final long numRestored) {

	}

	/**
	 * @see StateRestoreListener#onRestoreEnd(TopicPartition, String, long)
	 *
	 *      This method does nothing by default; if desired, subclasses should
	 *      override it with custom functionality.
	 *
	 */
	@Override
	public void onRestoreEnd(final T topicPartition, final String storeName, final long totalRestored) {

	}
}
