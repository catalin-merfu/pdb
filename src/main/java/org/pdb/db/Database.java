package org.pdb.db;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A database is a collection of text files indexed by the key indexers of its {@link Pdb} container.
 */
public class Database implements Closeable {

	static Logger logger = LoggerFactory.getLogger(Database.class);

	private Pdb pdb;
	private Path databasePath;
	private long commit;

	private Map<String, DatabaseIndex> indexNameToDatabaseIndexMap;

	private int readersCount;
	private Database nextDatabase;

	private AtomicLong transactionId;

	Database(Pdb pdb, Path databasePath, long commit) {

		this.pdb = pdb;
		this.databasePath = databasePath;
		this.commit = commit;

		indexNameToDatabaseIndexMap = DatabaseSupport.softValuesMap();
		
		transactionId = new AtomicLong();
		readersCount = 1;
	}

	/**
	 * Executes an indexed search of this database 
	 * 
	 * @param <K> Key type that must implements the Comparable interface
	 * @param indexName The name of an index provided in {@link Pdb}
	 * @param keys The list of keys 
	 * @return The records matching the keys in the input
	 * @throws IOException if an I/O error occurs when reading files
	 */
	public <K extends Comparable<K>> Stream<MatchedRecord<K>> lookup(String indexName, List<K> keys) throws IOException {

		@SuppressWarnings("unchecked")
		KeyIndex<K> keyIndex = (KeyIndex<K>) pdb.keyNameToKeyIndexMap.get(indexName);
		DatabaseIndex databaseIndex = getIndex(indexName);
		
		return databaseIndex.lookup(keys.stream().sorted(Comparator.naturalOrder()), keyIndex);
	}
	
	/**
	 * Executes an indexed search of this database returning the records in index order.
	 * 
	 * @param <K> Key type that must implements the Comparable interface
	 * @param indexName The name of an index provided in {@link Pdb}
	 * @return The index ordered records
	 * @throws IOException if an I/O error occurs when reading files
	 */
	public <K extends Comparable<K>> Stream<MatchedRecord<K>> streamOrdered(String indexName) throws IOException {

		@SuppressWarnings("unchecked")
		KeyIndex<K> keyIndex = (KeyIndex<K>) pdb.keyNameToKeyIndexMap.get(indexName);

		DatabaseIndex databaseIndex = getIndex(indexName);

		return databaseIndex.streamOrdered(keyIndex);
	}

	/**
	 * Executes an indexed search of this database returning the records in index reverse order.
	 * 
	 * @param <K> Key type that must implements the Comparable interface
	 * @param indexName The name of an index provided in {@link Pdb}
	 * @return The index reverse ordered records
	 * @throws IOException if an I/O error occurs when reading files
	 */
	public <K extends Comparable<K>> Stream<MatchedRecord<K>> streamReversed(String indexName) throws IOException {

		@SuppressWarnings("unchecked")
		KeyIndex<K> keyIndex = (KeyIndex<K>) pdb.keyNameToKeyIndexMap.get(indexName);

		DatabaseIndex databaseIndex = getIndex(indexName);

		return databaseIndex.streamReversed(keyIndex);
	}

	/**
	 * Executes an indexed search of this database returning the records in index order starting at an index value
	 * 
	 * @param <K> Key type that must implements the Comparable interface
	 * @param indexName The name of an index provided in {@link Pdb}
	 * @param greaterThan The index search starts above this key value
	 * @return The index ordered records that are greater than the provided key value
	 * @throws IOException if an I/O error occurs when reading files
	 */
	public <K extends Comparable<K>> Stream<MatchedRecord<K>> streamOrdered(String indexName, K greaterThan) throws IOException {

		@SuppressWarnings("unchecked")
		KeyIndex<K> keyIndex = (KeyIndex<K>) pdb.keyNameToKeyIndexMap.get(indexName);

		DatabaseIndex databaseIndex = getIndex(indexName);

		return databaseIndex.streamOrdered(keyIndex, greaterThan);
	}

	/**
	 * Executes an indexed search of this database returning the records in index reversed order starting at an index values
	 * 
	 * @param <K> Key type that must implements the Comparable interface
	 * @param indexName The name of an index provided in {@link Pdb}
	 * @param lessThan The index search starts below this key value
	 * @return The index ordered records that are less than the provided key value
	 * @throws IOException if an I/O error occurs when reading files
	 */
	public <K extends Comparable<K>> Stream<MatchedRecord<K>> streamReversed(String indexName, K lessThan) throws IOException {

		@SuppressWarnings("unchecked")
		KeyIndex<K> keyIndex = (KeyIndex<K>) pdb.keyNameToKeyIndexMap.get(indexName);

		DatabaseIndex databaseIndex = getIndex(indexName);

		return databaseIndex.streamReversed(keyIndex, lessThan);
	}

	/**
	 * Starts a transaction to update the database.
	 * 
	 * @return A database transaction
	 * @throws IOException if an I/O error occurs when modifying the file system
	 */
	public Transaction beginTransaction() throws IOException {
		return new Transaction(pdb, this, transactionId.incrementAndGet());
	}
	
	boolean notEmpty() throws IOException {
		try {
			return pdb.keyNameToKeyIndexMap.keySet().stream().anyMatch(indexName -> {

				DatabaseIndex databaseIndex;
				try {
					databaseIndex = getIndex(indexName);
				} catch (IOException e) {
					throw new UncheckedIOException(
							new IOException("Faild to load index " + indexName + " from database " + databasePath));
				}

				return databaseIndex.getFileNames().length != 0;
			});
		} catch (UncheckedIOException ex) {
			throw ex.getCause();
		}
	}

	DatabaseIndex getIndex(String indexName) throws IOException {

		try {
			return indexNameToDatabaseIndexMap.computeIfAbsent(indexName, name -> {
				try {
					DatabaseIndex databaseIndex;

					logger.debug("Start loading database index {} for database {}", name, databasePath.toString());
					databaseIndex = new DatabaseIndexLoader().load(name, databasePath, commit).internFields();
					logger.debug("Completed loading database index {} for database {}", name, databasePath.toString());

					return databaseIndex;
				} catch (IOException e) {
					throw new UncheckedIOException(new IOException(
							"Faild to load index " + name + " in database " + databasePath.toString(), e));
				}
			});
		} catch (UncheckedIOException ex) {
			throw ex.getCause();
		}
	}

	Path getDatabasePath() {
		return databasePath;
	}

	long getCommit() {
		return commit;
	}

	void setNextDatabase(Database nextDatabase) {
		this.nextDatabase = nextDatabase;
		nextDatabase.setTransactionId(transactionId);

		nextDatabase.readLock();
	}

	void setTransactionId(AtomicLong transactionId) {
		this.transactionId = transactionId;
	}

	synchronized Database readLock() {
		readersCount++;
		
		return this;
	}

	synchronized void readUnlock() throws IOException  {

		readersCount--;
		if(readersCount == 0) {

			Path removeFilesPath = DatabaseSupport.getDatabaseRemoveFilesIndexPath(databasePath, commit);
			if(Files.exists(removeFilesPath)) {

				try {
					for(String indexName: pdb.keyNameToKeyIndexMap.keySet()) {
						Path indexPath = DatabaseSupport.getDatabaseIndexPath(databasePath, indexName, commit);
						Files.deleteIfExists(indexPath);

						Path fileIndexPath = DatabaseSupport.getDatabaseFileIndexPath(databasePath, indexName, commit);
						Files.deleteIfExists(fileIndexPath);
					}

					Path dataPath = DatabaseSupport.getDatabaseDataDirectoryPath(databasePath);
					try {
						FileSystem fileSystem = databasePath.getFileSystem();
						Files.lines(removeFilesPath).forEach(relativeFileName -> {
							Path relativeFilePath = fileSystem.getPath(relativeFileName);

							for(String indexName: pdb.keyNameToKeyIndexMap.keySet()) {
								Path fileIndexPath = DatabaseSupport.getFileIndexPath(databasePath, relativeFilePath, indexName);

								try {
									Files.deleteIfExists(fileIndexPath);
								} catch (IOException e) {
									throw new UncheckedIOException(new IOException("Failed to remove index file " + fileIndexPath.toString(), e));
								}
							}

							try {
								Files.deleteIfExists(DatabaseSupport.getDatabaseDataIndexDirectoryPath(databasePath).resolve(relativeFilePath));
								Files.deleteIfExists(dataPath.resolve(relativeFilePath));
							} catch (IOException e) {
								throw new UncheckedIOException(new IOException("Failed to remove file " + relativeFilePath.toString(), e));
							}
						});
					} catch (UncheckedIOException e) {
						throw e.getCause();
					}
					catch (IOException e) {
						throw new IOException("Failed to read file " + removeFilesPath.toString(), e);
					}

					try {
						Files.delete(removeFilesPath);
					} catch (IOException e) {
						throw new IOException("Failed to remove file " + removeFilesPath.toString(), e);
					}
					
					Files.delete(DatabaseSupport.getDatabaseCommitPath(databasePath, commit));
				}
				finally {
					nextDatabase.readUnlock();
				}
			}
			else if(pdb.isDatabaseDeleted(databasePath))
					DatabaseSupport.deleteDatabaseDirectories(databasePath);
			else
				Files.delete(DatabaseSupport.getDatabaseCommitPath(databasePath, commit));
		}
	}

	@Override
	public void close() throws IOException {
		readUnlock();
	}
}
