package luva.xx.kafka.kjm.db.processor;

/**
 * @author Hulva Luva.H
 * @date 2019年2月23日
 * @description
 *
 */
public interface StateRestoreListener<T> {
	/**
	 * Method called at the very beginning of {@link StateStore} restoration.
	 *
	 * @param topicPartition the TopicPartition containing the values to restore
	 * @param storeName      the name of the store undergoing restoration
	 * @param startingOffset the starting offset of the entire restoration process
	 *                       for this TopicPartition
	 * @param endingOffset   the exclusive ending offset of the entire restoration
	 *                       process for this TopicPartition
	 */
	void onRestoreStart(final T topicPartition, final String storeName, final long startingOffset, final long endingOffset);

	/**
	 * Method called after restoring a batch of records. In this case the maximum
	 * size of the batch is whatever the value of the MAX_POLL_RECORDS is set to.
	 *
	 * This method is called after restoring each batch and it is advised to keep
	 * processing to a minimum. Any heavy processing will hold up recovering the
	 * next batch, hence slowing down the restore process as a whole.
	 *
	 * If you need to do any extended processing or connecting to an external
	 * service consider doing so asynchronously.
	 *
	 * @param topicPartition the TopicPartition containing the values to restore
	 * @param storeName      the name of the store undergoing restoration
	 * @param batchEndOffset the inclusive ending offset for the current restored
	 *                       batch for this TopicPartition
	 * @param numRestored    the total number of records restored in this batch for
	 *                       this TopicPartition
	 */
	void onBatchRestored(final T topicPartition, final String storeName, final long batchEndOffset, final long numRestored);

	/**
	 * Method called when restoring the {@link StateStore} is complete.
	 *
	 * @param topicPartition the TopicPartition containing the values to restore
	 * @param storeName      the name of the store just restored
	 * @param totalRestored  the total number of records restored for this
	 *                       TopicPartition
	 */
	void onRestoreEnd(final T topicPartition, final String storeName, final long totalRestored);
}
