package luva.xx.kafka.kjm.db.processor;

import java.util.AbstractMap;
import java.util.Collection;

/**
 * @author Hulva Luva.H
 * @date 2019年2月23日
 * @description
 *
 */
public interface BatchingStateRestoreCallback extends StateRestoreCallback {

	/**
	 * Called to restore a number of records. This method is called repeatedly until
	 * the {@link StateStore} is fulled restored.
	 *
	 * @param records the records to restore.
	 */
	void restoreAll(Collection<AbstractMap.SimpleEntry<byte[], byte[]>> records);

}