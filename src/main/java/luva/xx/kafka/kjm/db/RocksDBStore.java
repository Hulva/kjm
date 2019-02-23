package luva.xx.kafka.kjm.db;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import luva.xx.kafka.kjm.db.processor.AbstractNotifyingBatchingRestoreCallback;

/**
 * @author Hulva Luva.H
 * @date 2019年2月22日
 * @description
 *
 */
public class RocksDBStore implements KeyValueStore<Bytes, byte[]> {
	private final static Logger LOGGER = LoggerFactory.getLogger(RocksDBStore.class);

	private static final Pattern SST_FILE_EXTENSION = Pattern.compile(".*\\.sst");

	private static final CompressionType COMPRESSION_TYPE = CompressionType.NO_COMPRESSION;
	private static final CompactionStyle COMPACTION_STYLE = CompactionStyle.UNIVERSAL;
	private static final long WRITE_BUFFER_SIZE = 16 * 1024 * 1024L;
	private static final long BLOCK_CACHE_SIZE = 50 * 1024 * 1024L;
	private static final long BLOCK_SIZE = 4096L;
	private static final int MAX_WRITE_BUFFERS = 3;

	RocksDBAccessor dbAccessor;
	final Set<KeyValueIterator<Bytes, byte[]>> openIterators = Collections.synchronizedSet(new HashSet<>());
	// the following option objects will be created in the constructor and closed in
	// the close() method
	private RocksDBGenericOptionsToDbOptionsColumnFamilyOptionsAdapter userSpecifiedOptions;
	WriteOptions wOptions;
	FlushOptions fOptions;

	private static final String DB_PATH = "data";
	private static final String name = "kjm";
	RocksDB DB;
	static RocksDBStore database;
	protected volatile boolean open = false;
	static {
//		RocksDB.loadLibrary();
		try {
			Files.createDirectories(Paths.get(DB_PATH + "/" + name));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private RocksDBStore() {
	}

	public static RocksDBStore build() {
		if (database == null) {
			database = new RocksDBStore();
			database.openDB();
		}
		return database;
	}

	public RocksDB getDB() {
		return DB;
	}

	void openDB() {
		// initialize the default rocksdb options

		final DBOptions dbOptions = new DBOptions();
		final ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
		userSpecifiedOptions = new RocksDBGenericOptionsToDbOptionsColumnFamilyOptionsAdapter(dbOptions, columnFamilyOptions);

		final BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
		tableConfig.setBlockCacheSize(BLOCK_CACHE_SIZE);
		tableConfig.setBlockSize(BLOCK_SIZE);
		tableConfig.setFilter(new BloomFilter());

		userSpecifiedOptions.optimizeFiltersForHits();
		userSpecifiedOptions.setTableFormatConfig(tableConfig);
		userSpecifiedOptions.setWriteBufferSize(WRITE_BUFFER_SIZE);
		userSpecifiedOptions.setCompressionType(COMPRESSION_TYPE);
		userSpecifiedOptions.setCompactionStyle(COMPACTION_STYLE);
		userSpecifiedOptions.setMaxWriteBufferNumber(MAX_WRITE_BUFFERS);
		userSpecifiedOptions.setCreateIfMissing(true);
		userSpecifiedOptions.setErrorIfExists(false);
		userSpecifiedOptions.setInfoLogLevel(InfoLogLevel.ERROR_LEVEL);
		// this is the recommended way to increase parallelism in RocksDb
		// note that the current implementation of setIncreaseParallelism affects the
		// number
		// of compaction threads but not flush threads (the latter remains one). Also
		// the parallelism value needs to be at least two because of the code in
		// https://github.com/facebook/rocksdb/blob/62ad0a9b19f0be4cefa70b6b32876e764b7f3c11/util/options.cc#L580
		// subtracts one from the value passed to determine the number of compaction
		// threads
		// (this could be a bug in the RocksDB code and their devs have been contacted).
		userSpecifiedOptions.setIncreaseParallelism(Math.max(Runtime.getRuntime().availableProcessors(), 2));

		wOptions = new WriteOptions();
		wOptions.setDisableWAL(true);

		fOptions = new FlushOptions();
		fOptions.setWaitForFlush(true);

		userSpecifiedOptions.prepareForBulkLoad();

		openRocksDB(dbOptions, columnFamilyOptions);
		open = true;
	}

	void openRocksDB(final DBOptions dbOptions, final ColumnFamilyOptions columnFamilyOptions) {
		final List<ColumnFamilyDescriptor> columnFamilyDescriptors = Collections
				.singletonList(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions));
		final List<ColumnFamilyHandle> columnFamilies = new ArrayList<>(columnFamilyDescriptors.size());

		try {
			DB = RocksDB.open(dbOptions, DB_PATH + "/" + name, columnFamilyDescriptors, columnFamilies);
			dbAccessor = new SingleColumnFamilyAccessor(columnFamilies.get(0));
		} catch (final RocksDBException e) {
			throw new RuntimeException("Error opening store " + name + " at location "
					+ Paths.get(DB_PATH + "/" + name).toAbsolutePath().toFile().getAbsolutePath(), e);
		}
	}

	void write(final WriteBatch batch) throws RocksDBException {
		DB.write(wOptions, batch);
	}

	void toggleDbForBulkLoading(final boolean prepareForBulkload) {
		if (prepareForBulkload) {
			// if the store is not empty, we need to compact to get around the num.levels
			// check for bulk loading
			final String[] sstFileNames = new File(DB_PATH + "/" + name).list((dir, name) -> SST_FILE_EXTENSION.matcher(name).matches());

			if (sstFileNames != null && sstFileNames.length > 0) {
				dbAccessor.toggleDbForBulkLoading();
			}
		}
		close();
		openDB();
	}

	interface RocksDBAccessor {

		void put(final byte[] key, final byte[] value);

		void prepareBatch(final List<AbstractMap.SimpleEntry<Bytes, byte[]>> entries, final WriteBatch batch) throws RocksDBException;

		byte[] get(final byte[] key) throws RocksDBException;

		/**
		 * In contrast to get(), we don't migrate the key to new CF.
		 * <p>
		 * Use for get() within delete() -- no need to migrate, as it's deleted anyway
		 */
		byte[] getOnly(final byte[] key) throws RocksDBException;

		KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to);

		KeyValueIterator<Bytes, byte[]> all();

		long approximateNumEntries() throws RocksDBException;

		void flush() throws RocksDBException;

		void prepareBatchForRestore(final Collection<AbstractMap.SimpleEntry<byte[], byte[]>> records, final WriteBatch batch)
				throws RocksDBException;

		void close();

		void toggleDbForBulkLoading();
	}

	class SingleColumnFamilyAccessor implements RocksDBAccessor {
		private final ColumnFamilyHandle columnFamily;

		SingleColumnFamilyAccessor(final ColumnFamilyHandle columnFamily) {
			this.columnFamily = columnFamily;
		}

		@Override
		public void put(final byte[] key, final byte[] value) {
			if (value == null) {
				try {
					DB.delete(columnFamily, wOptions, key);
				} catch (final RocksDBException e) {
					// String format is happening in wrapping stores. So formatted message is thrown
					// from wrapping stores.
					throw new RuntimeException("Error while removing key from store " + name, e);
				}
			} else {
				try {
					DB.put(columnFamily, wOptions, key, value);
				} catch (final RocksDBException e) {
					// String format is happening in wrapping stores. So formatted message is thrown
					// from wrapping stores.
					throw new RuntimeException("Error while putting key/value into store " + name, e);
				}
			}
		}

		@Override
		public void prepareBatch(final List<AbstractMap.SimpleEntry<Bytes, byte[]>> entries, final WriteBatch batch)
				throws RocksDBException {
			for (final AbstractMap.SimpleEntry<Bytes, byte[]> entry : entries) {
				Objects.requireNonNull(entry.getKey(), "key cannot be null");
				if (entry.getValue() == null) {
					batch.delete(columnFamily, entry.getKey().get());
				} else {
					batch.put(columnFamily, entry.getKey().get(), entry.getValue());
				}
			}
		}

		@Override
		public byte[] get(final byte[] key) throws RocksDBException {
			return DB.get(columnFamily, key);
		}

		@Override
		public byte[] getOnly(final byte[] key) throws RocksDBException {
			return DB.get(columnFamily, key);
		}

		@Override
		public KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to) {
			return new RocksDBRangeIterator(name, DB.newIterator(columnFamily), openIterators, from, to);
		}

		@Override
		public KeyValueIterator<Bytes, byte[]> all() {
			final RocksIterator innerIterWithTimestamp = DB.newIterator(columnFamily);
			innerIterWithTimestamp.seekToFirst();
			return new RocksDbIterator(name, innerIterWithTimestamp, openIterators);
		}

		@Override
		public long approximateNumEntries() throws RocksDBException {
			return DB.getLongProperty(columnFamily, "rocksdb.estimate-num-keys");
		}

		@Override
		public void flush() throws RocksDBException {
			DB.flush(fOptions, columnFamily);
		}

		@Override
		public void prepareBatchForRestore(final Collection<AbstractMap.SimpleEntry<byte[], byte[]>> records, final WriteBatch batch)
				throws RocksDBException {
			for (final AbstractMap.SimpleEntry<byte[], byte[]> record : records) {
				if (record.getValue() == null) {
					batch.delete(columnFamily, record.getKey());
				} else {
					batch.put(columnFamily, record.getKey(), record.getValue());
				}
			}
		}

		@Override
		public void close() {
			columnFamily.close();
		}

		@Override
		public void toggleDbForBulkLoading() {
			try {
				DB.compactRange(columnFamily);
			} catch (final RocksDBException e) {
				throw new RuntimeException("Error while range compacting during restoring  store " + name, e);
			}
		}
	}

	// not private for testing
	static class RocksDBBatchingRestoreCallback extends AbstractNotifyingBatchingRestoreCallback<Map<String, String>> {

		private final RocksDBStore rocksDBStore;

		RocksDBBatchingRestoreCallback(final RocksDBStore rocksDBStore) {
			this.rocksDBStore = rocksDBStore;
		}

		@Override
		public void restoreAll(final Collection<AbstractMap.SimpleEntry<byte[], byte[]>> records) {
			try (final WriteBatch batch = new WriteBatch()) {
				rocksDBStore.dbAccessor.prepareBatchForRestore(records, batch);
				rocksDBStore.write(batch);
			} catch (final RocksDBException e) {
				throw new RuntimeException("Error restoring batch to store " + name, e);
			}
		}

		@Override
		public void onRestoreStart(final Map<String, String> topicPartition, final String storeName, final long startingOffset,
				final long endingOffset) {
			rocksDBStore.toggleDbForBulkLoading(true);
		}

		@Override
		public void onRestoreEnd(final Map<String, String> topicPartition, final String storeName, final long totalRestored) {
			rocksDBStore.toggleDbForBulkLoading(false);
		}
	}

	public synchronized void close() {
		if (!open) {
			return;
		}
		open = false;
		closeOpenIterators();
		dbAccessor.close();
		userSpecifiedOptions.close();
		wOptions.close();
		fOptions.close();
		DB.close();

		dbAccessor = null;
		userSpecifiedOptions = null;
		wOptions = null;
		fOptions = null;
		DB = null;
	}

	private void closeOpenIterators() {
		final HashSet<KeyValueIterator<Bytes, byte[]>> iterators;
		synchronized (openIterators) {
			iterators = new HashSet<>(openIterators);
		}
		if (iterators.size() != 0) {
			LOGGER.warn("Closing {} open iterators for store {}", iterators.size(), name);
			for (final KeyValueIterator<Bytes, byte[]> iterator : iterators) {
				iterator.close();
			}
		}
	}

	private void validateStoreOpen() {
		if (!open) {
			throw new RuntimeException("Store " + name + " is currently closed");
		}
	}

	@Override
	public String name() {
		return name;
	}

	@Override
	public synchronized void flush() {
		if (DB == null) {
			return;
		}
		try {
			dbAccessor.flush();
		} catch (final RocksDBException e) {
			throw new RuntimeException("Error while executing flush from store " + name, e);
		}
	}

	@Override
	public boolean persistent() {
		return true;
	}

	@Override
	public boolean isOpen() {
		return open;
	}

	@Override
	public synchronized byte[] get(final Bytes key) {
		validateStoreOpen();
		try {
			return dbAccessor.get(key.get());
		} catch (final RocksDBException e) {
			// String format is happening in wrapping stores. So formatted message is thrown
			// from wrapping stores.
			throw new RuntimeException("Error while getting value for key from store " + name, e);
		}
	}

	@Override
	public synchronized KeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to) {
		Objects.requireNonNull(from, "from cannot be null");
		Objects.requireNonNull(to, "to cannot be null");
		validateStoreOpen();

		final KeyValueIterator<Bytes, byte[]> rocksDBRangeIterator = dbAccessor.range(from, to);
		openIterators.add(rocksDBRangeIterator);

		return rocksDBRangeIterator;
	}

	@Override
	public synchronized KeyValueIterator<Bytes, byte[]> all() {
		validateStoreOpen();
		final KeyValueIterator<Bytes, byte[]> rocksDbIterator = dbAccessor.all();
		openIterators.add(rocksDbIterator);
		return rocksDbIterator;
	}

	@Override
	public long approximateNumEntries() {
		validateStoreOpen();
		final long numEntries;
		try {
			numEntries = dbAccessor.approximateNumEntries();
		} catch (final RocksDBException e) {
			throw new RuntimeException("Error fetching property from store " + name, e);
		}
		if (isOverflowing(numEntries)) {
			return Long.MAX_VALUE;
		}
		return numEntries;
	}

	private boolean isOverflowing(final long value) {
		// RocksDB returns an unsigned 8-byte integer, which could overflow long
		// and manifest as a negative value.
		return value < 0;
	}

	@Override
	public synchronized void put(final Bytes key, final byte[] value) {
		Objects.requireNonNull(key, "key cannot be null");
		validateStoreOpen();
		dbAccessor.put(key.get(), value);
	}

	@Override
	public synchronized byte[] putIfAbsent(final Bytes key, final byte[] value) {
		Objects.requireNonNull(key, "key cannot be null");
		final byte[] originalValue = get(key);
		if (originalValue == null) {
			put(key, value);
		}
		return originalValue;
	}

	@Override
	public void putAll(final List<SimpleEntry<Bytes, byte[]>> entries) {
		try (final WriteBatch batch = new WriteBatch()) {
			dbAccessor.prepareBatch(entries, batch);
			write(batch);
		} catch (final RocksDBException e) {
			throw new RuntimeException("Error while batch writing to store " + name, e);
		}
	}

	@Override
	public synchronized byte[] delete(final Bytes key) {
		Objects.requireNonNull(key, "key cannot be null");
		final byte[] oldValue;
		try {
			oldValue = dbAccessor.getOnly(key.get());
		} catch (final RocksDBException e) {
			// String format is happening in wrapping stores. So formatted message is thrown
			// from wrapping stores.
			throw new RuntimeException("Error while getting value for key from store " + name, e);
		}
		put(key, null);
		return oldValue;
	}

}
