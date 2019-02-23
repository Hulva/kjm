package luva.xx.kafka.kjm.db.processor;

import luva.xx.kafka.kjm.annotation.InterfaceStability;

/**
 * @author Hulva Luva.H
 * @date 2019年2月23日
 * @description
 *
 */
@InterfaceStability.Evolving
public interface StateRestoreCallback {
	void restore(byte[] key, byte[] value);
}
