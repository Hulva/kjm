package luva.xx.kafka.kjm.test.rocksdb;

import org.junit.Test;
import org.rocksdb.ColumnFamilyHandle;

import luva.xx.kafka.kjm.db.Database;

/**
 * @author Hulva Luva.H
 * @date 2019年2月23日
 * @description
 *
 */
public class TestRocksDB {

	@Test
	public void test() {
		Database.build();
		for (final ColumnFamilyHandle columnFamilyHandle : Database.columnFamilyHandleList) {
			System.out.println(columnFamilyHandle);
		}
	}

}
