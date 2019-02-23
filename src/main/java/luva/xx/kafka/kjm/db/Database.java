package luva.xx.kafka.kjm.db;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Hulva Luva.H
 * @date 2019年2月22日
 * @description
 *
 */
public class Database implements Closeable {
	private final static Logger LOGGER = LoggerFactory.getLogger(Database.class);

	private final static DBOptions options = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true)
			.setMaxOpenFiles(200)
//			.setParanoidChecks(paranoidChecks)
	;
	final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions().optimizeUniversalStyleCompaction();
	// list of column family descriptors, first entry must always be default column
	// family
	// a list which will hold the handles for the column families once the db is
	// opened
	public static final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
	final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts),
			new ColumnFamilyDescriptor("kjm".getBytes(), cfOpts));

	private static final String DB_PATH = "data/kjm";

	private static RocksDB DB;
	private static Database database;
	static {
		RocksDB.loadLibrary();
		try {
			Files.createDirectories(Paths.get(DB_PATH));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private Database() {
	}

	public static Database build() {
		if (database == null) {
			database = new Database();
			database.init();
		}
		return database;
	}

	private void init() {
		try {
			DB = RocksDB.open(options, DB_PATH, cfDescriptors, columnFamilyHandleList);
		} catch (RocksDBException e) {
			LOGGER.error("Error open RocksDB.", e);
		}
	}

	public static RocksDB getDB() {
		return DB;
	}

	@Override
	public void close() throws IOException {
		for (final ColumnFamilyHandle columnFamilyHandle : columnFamilyHandleList) {
			columnFamilyHandle.close();
		}
		if (DB != null) {
			DB.close();
		}
		if (options != null) {
			options.close();
		}
		if (cfOpts != null) {
			cfOpts.close();
		}
	}

}
