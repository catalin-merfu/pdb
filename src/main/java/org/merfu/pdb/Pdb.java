package org.merfu.pdb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a container for databases indexed by the same set if key indexes
 */
public class Pdb {

	static private Logger logger = LoggerFactory.getLogger(Pdb.class);

	private Path pdbPath;
	KeyIndex<?>[] keyIndexes;

	Map<String, KeyIndex<?>> keyNameToKeyIndexMap;

	Map<String, Database> databasesMap;

	/**
	 * Initializes a new databases container or loads the databases at the provided path if one was created previously.
	 * 
	 * @param pdbPath The root path of this database container
	 * @param keyIndexes The indexes that are used to index the databases in this container
	 * @throws IOException if an I/O error occurs when reading or writing the file system
	 */
	public Pdb(Path pdbPath, KeyIndex<?>[] keyIndexes) throws IOException {

		this.pdbPath = pdbPath;
		Files.createDirectories(pdbPath);

		this.keyIndexes = keyIndexes;

		keyNameToKeyIndexMap = new HashMap<>();

		Arrays.stream(keyIndexes).forEach(keyIndex -> {
			String indexName = keyIndex.getName();

			keyNameToKeyIndexMap.put(indexName, keyIndex);
		});

		databasesMap = DatabaseSupport.softValuesMap();

		recoverDatabases();
	}

	/**
	 * Returns the index with the name that was provided when this container was created
	 * 
	 * @param indexName The index name
	 * @return The index for the requested index name
	 */
	public KeyIndex<?> getKeyIndex(String indexName) {
		return keyNameToKeyIndexMap.get(indexName);
	}

	/**
	 * Deletes a database in this container
	 * 
	 * @param databaseName The unique name of the database in this container
	 * @return The database for the name
	 * @throws IOException if an I/O error occurs when reading the file system
	 */
	public Database getDatabase(String databaseName) throws IOException {
		try {
			return databasesMap.compute(databaseName, (name, db) -> {
				if(db != null) 
					return db.readLock();
	
				Path databasePath = pdbPath.resolve(name);
				if(!Files.isDirectory(databasePath))
					return null;
				
				try {
					return lastDatabase(databasePath).get().readLock();
				} catch (IOException e) {
					throw new UncheckedIOException(e);
				}
			});
		}
		catch(UncheckedIOException ex) {
			throw ex.getCause();
		}
	}

	/**
	 * Returns the name of the databases hosted by this container
	 * 
	 * @return The names of databases in this container 
	 * @throws IOException if an I/O error occurs when reading the file system
	 */
	public Set<String> getInstalledDatabases() throws IOException {
		try(Stream<Path> pdbStream = Files.list(pdbPath)) {
			return pdbStream.filter(Files::isDirectory).map(databasePath -> databasePath.getFileName().toString()).collect(Collectors.toSet());
		}
	}

	/**
	 * Creates a new database
	 * 
	 * @param databaseName The name of the database unique to this container
	 * @throws IOException if an I/O error occurs when reading or writing the file system
	 */
	public void createDatabase(String databaseName) throws IOException {

		databasesMap.compute(databaseName, (name, database) -> {

			Path databasePath = pdbPath.resolve(databaseName);
			if(Files.exists(databasePath))
				return database;

			try {
				Files.createDirectory(databasePath);

				Path deletedPath = DatabaseSupport.getDatabaseDeletedPath(databasePath);
				Files.createFile(deletedPath);

				Path txPath = DatabaseSupport.getDatabaseTxDirectoryPath(databasePath);
				Files.createDirectory(txPath);

				Path databaseDataPath = DatabaseSupport.getDatabaseDataDirectoryPath(databasePath);
				Files.createDirectory(databaseDataPath);

				// create empty indexes with commit 0
				new DatabaseIndexer(this, databasePath, 0).index();

				Path commitPath = DatabaseSupport.getDatabaseCommitPath(databasePath, 0L);
				Files.createFile(commitPath);

				Files.delete(deletedPath);
			}
			catch(IOException ex) {
				try {
					DatabaseSupport.deleteDatabaseDirectories(databasePath);
				} catch (IOException e) {
					logger.debug("Failed to cleanup database {}", databasePath, e);
				}
			}

			return null;
		});
	}

	/**
	 * Deletes a database.
	 * 
	 * @param databaseName The database name
	 * @throws IOException if an I/O error occurs when reading or writing the file system
	 */
	public void deleteDatabase(String databaseName) throws IOException {

		try {
			databasesMap.compute(databaseName, (name, database) -> {
				
				Path databasePath = pdbPath.resolve(databaseName);
				if(Files.isDirectory(databasePath) && !isDatabaseDeleted(databasePath)) {
					try {
						deleteDatabase(databasePath);
						if(database != null)
							database.readUnlock();
						else {
							lastDatabase(databasePath).get().readUnlock();
						}
						
					} catch (IOException e) {
						throw new UncheckedIOException(e);
					}
				}

				return null;
			});
		}
		catch(UncheckedIOException e) {
			throw e.getCause();
		}
	}

	private void deleteDatabase(Path databasePath) throws IOException {
		Path deletedPath = DatabaseSupport.getDatabaseDeletedPath(databasePath);
		Files.createFile(deletedPath);
	}

	boolean isDatabaseDeleted(Path databasePath) {
		return Files.isRegularFile(DatabaseSupport.getDatabaseDeletedPath(databasePath))
				|| !Files.isDirectory(DatabaseSupport.getDatabaseDataDirectoryPath(databasePath));
	}

	private void recoverDatabases() throws IOException {

		try (Stream<Path> databasePaths = Files.list(pdbPath)) {
			databasePaths.filter(Files::isDirectory).forEach(databasePath -> {
				try {
					recoverDatabase(databasePath);
				} catch (IOException e) {
					throw new UncheckedIOException(new IOException("Failed to load database " + databasePath, e));
				}
			});
		} catch (UncheckedIOException e) {
			throw e.getCause();
		}
	}

	private void recoverDatabase(Path databasePath) throws IOException {

		if (isDatabaseDeleted(databasePath)) {
			DatabaseSupport.deleteDatabaseDirectories(databasePath);
			return;
		}

		Optional<Database> optionalDatabase = lastDatabase(databasePath);
		if(optionalDatabase.isEmpty()) {
			DatabaseSupport.removeDirectory(databasePath);
			return;
		}

		Database database = optionalDatabase.get();
		long commit = database.getCommit();
		
		Path commitDatabasePath = DatabaseSupport.getDatabaseCommitDirectoryPath(databasePath);
		Database commitDatabase;
		if(Files.exists(commitDatabasePath)) {
			try (Database commitedDatabase = new Database(this, commitDatabasePath, 0))  {
				if(!isDatabaseDeleted(commitDatabasePath)) {
					
					OptionalLong optionalCommit = DatabaseSupport.getDatabaseCommitPath(commitDatabasePath);
					if(optionalCommit.isEmpty()) {
						optionalCommit = OptionalLong.of(commit);
						Files.createFile(DatabaseSupport.getDatabaseCommitPath(commitDatabasePath, commit));
					}
					
					if(commit == optionalCommit.getAsLong()) {
						commitDatabase = mergeCommit(database, commitedDatabase);
					}
					else {
						commitDatabase = database;
						deleteDatabase(commitDatabasePath);
					}
				}
				else
					commitDatabase = database;
			}
		}
		else {
			commitDatabase = database;
		}

		Path txPath = DatabaseSupport.getDatabaseTxDirectoryPath(databasePath);
		try(Stream<Path> txStream = Files.list(txPath)) {

			txStream.filter(Files::isDirectory).map(new Function<Path, Database>() {
				private Database lastDb = commitDatabase;
				
				public Database apply(Path transactionPath) {
					try {
						Path committedPath = DatabaseSupport.getDatabaseCommittedPath(transactionPath);
						if (Files.exists(committedPath)) {
							new DatabaseIndexer(Pdb.this, transactionPath, 0).index();
							lastDb = commitTransaction(lastDb, databasePath, transactionPath);
						}
						else {
							rollbackTransaction(transactionPath);
						}
					}
					catch(IOException ex) {
						throw new UncheckedIOException(ex);
					}
					
					return lastDb;
				}
			}).reduce((d1, d2) -> d2).orElse(database);
		}
	}

	private Optional<Database> lastDatabase(Path databasePath) throws IOException {
		
		try (Stream<Path> indexPaths = Files.list(DatabaseSupport.getDatabaseIndexDirectoryPath(databasePath))) {
			return indexPaths.filter(Files::isRegularFile)
				.filter(indexPath -> indexPath.getFileName().toString().matches("commit\\.\\d+"))
				.mapToLong(file -> new VersionFile(file).getVersion()).sorted().<Database>mapToObj(new LongFunction<>() {
					Database currentDatabase;
					
					public Database apply(long commit) {
						currentDatabase = mergeNewDatabase(currentDatabase, loadDatabase(databasePath, commit));

						return currentDatabase;
					}
				}).reduce((db1, db2) -> db2);
		}
	}
	
	Database commitTransaction(String commitDatabaseName, Path databasePath, Path txPath) throws IOException {
		try {
			return databasesMap.compute(commitDatabaseName, (databaseName, currentDatabase) -> {
			
				if(currentDatabase == null) {
					// this cannot happen 
					if(!Files.isDirectory(databasePath))
						// this can only happen when the database was deleted;
						throw new UncheckedIOException(new IOException("Database does not exist: " + databasePath.toString()));
	
					try {
						currentDatabase = lastDatabase(databasePath).get();
					} catch (IOException e) {
						throw new UncheckedIOException(e);
					}
				}
	
				try {
					return commitTransaction(currentDatabase, databasePath, txPath);
				} catch (IOException e) {
					throw new UncheckedIOException(e);
				}
			});
		}
		catch(UncheckedIOException ex) {
			throw ex.getCause();
		}
	}

	private Database commitTransaction(Database currentDatabase, Path databasePath, Path txPath) throws IOException {

		Path commitDatabasePath = DatabaseSupport.getDatabaseCommitDirectoryPath(databasePath);

		Files.move(txPath, commitDatabasePath);

		Files.createFile(DatabaseSupport.getDatabaseCommitPath(commitDatabasePath, currentDatabase.getCommit()));
		
		try (Database commitDatabase = new Database(this, commitDatabasePath, 0))  {
			return mergeCommit(currentDatabase, commitDatabase);
		}
	}

	void rollbackTransaction(Path txPath) throws IOException {
		DatabaseSupport.deleteDatabaseDirectories(txPath);
	}

	private Database mergeCommit(Database database, Database commitDatabase) throws IOException {

		Path commitDatabasePath = commitDatabase.getDatabasePath();

		try {
			long commit = database.getCommit();
			long nextCommit = commit + 1;

			Path databasePath = database.getDatabasePath();
			FileSystem fileSystem = databasePath.getFileSystem();

			Path removeFilesPath = DatabaseSupport.getDatabaseRemoveFilesIndexPath(databasePath, commit);
			Set<String> removeFilesSet = new HashSet<>();

			Stream.of(keyIndexes).forEach(keyIndex -> {
				String indexName = keyIndex.getName();

				Path nextDatabaseIndexPath = DatabaseSupport.getDatabaseIndexPath(databasePath, indexName,
						nextCommit);
				Path nextDatabaseFileIndexPath = DatabaseSupport.getDatabaseFileIndexPath(databasePath, indexName,
						nextCommit);

				try {
					if(commit == 0) {
						if(!Files.isRegularFile(nextDatabaseIndexPath)) {
							Path commitDatabaseIndexPath = DatabaseSupport.getDatabaseIndexPath(commitDatabasePath, indexName, 0);
							Files.createLink(nextDatabaseIndexPath, commitDatabaseIndexPath);
						}

						Path commitDatabaseFileIndexPath = DatabaseSupport.getDatabaseFileIndexPath(commitDatabasePath, indexName, 0);
						Map<String,String> fileNameToIdMap = new HashMap<>();
						try(BufferedReader reader = Files.newBufferedReader(commitDatabaseFileIndexPath)) {
							String line = reader.readLine();
							while(line != null) {
								String[] fields = line.split("\\|");
								fileNameToIdMap.put(fields[1] + "." + nextCommit, fields[0]);

								line = reader.readLine();
							}
						}

						new DatabaseFileIndexPersister().persist(nextDatabaseFileIndexPath, fileNameToIdMap);
					}
					else {
						DatabaseIndex commitDatabaseIndex = commitDatabase.getIndex(indexName);
						String[] commitFileNames = commitDatabaseIndex.getFileNames();

						if (commitFileNames.length == 0) {
							if(!Files.isRegularFile(nextDatabaseIndexPath)) {
								Path databaseIndexPath = DatabaseSupport.getDatabaseIndexPath(databasePath, indexName,
										commit);
								Files.createLink(nextDatabaseIndexPath, databaseIndexPath);
							}

							if(!Files.isRegularFile(nextDatabaseFileIndexPath)) {
								Path fileIndexPath = DatabaseSupport.getDatabaseFileIndexPath(databasePath, indexName,
										commit);
								Files.createLink(nextDatabaseFileIndexPath, fileIndexPath);
							}
						}
						else {
							DatabaseIndex databaseIndex = database.getIndex(indexName);
							DatabaseIndexEntry[] databaseIndexEntries = databaseIndex.getEntries();
	
							DatabaseIndexEntry[] commitDatabaseIndexEntries = commitDatabaseIndex.getEntries();
	
							Map<String, List<String>> keystringToPathIdListMap;
							Map<String, String> fileNameToIdMap = new HashMap<>();

							Set<String> replacedFileNames = Stream.of(commitFileNames).collect(Collectors.toSet());
							
							logger.debug("Starting merging database index '{}' under {}...", indexName, databasePath.toString());
							keystringToPathIdListMap = Stream.concat(Stream.of(databaseIndexEntries)
									.flatMap(entry -> entry.getFileNames().filter(fileName -> {
										if (replacedFileNames.contains(
												new VersionFile(fileSystem.getPath(fileName)).getBasename())) {
											removeFilesSet.add(fileName);
											return false;
										}
										return true;
									}).map(fileName -> new SimpleImmutableEntry<>(entry.getKeyString(), fileName))),
									Stream.of(commitDatabaseIndexEntries)
											.flatMap(entry -> entry.getFileNames()
													.map(fileName -> new SimpleImmutableEntry<>(entry.getKeyString(),
															fileName + "." + nextCommit))))
									.map(new Function<SimpleImmutableEntry<String, String>, SimpleImmutableEntry<String, String>>() {
										private int nextFileId;
										
										public SimpleImmutableEntry<String, String> apply(SimpleImmutableEntry<String, String> entry) {
											String dataFileId = fileNameToIdMap.computeIfAbsent(entry.getValue(),
													fileName -> Integer.toString(++nextFileId, Character.MAX_RADIX));
		
											return new SimpleImmutableEntry<>(entry.getKey(), dataFileId);
										}
									}).collect(Collectors.groupingBy(SimpleImmutableEntry::getKey,
											Collectors.mapping(SimpleImmutableEntry::getValue, SinglyLinkedList.collector())));
							logger.debug("Completed merging database index '{}' under {}", indexName, databasePath.toString());

							logger.debug("Starting saving database index '{}' under {}...", indexName, databasePath.toString());
							new DatabaseIndexPersister().persist(nextDatabaseIndexPath, keyIndex,
									keystringToPathIdListMap);
							new DatabaseFileIndexPersister().persist(nextDatabaseFileIndexPath, fileNameToIdMap);
							logger.debug("Completed saving database index '{}' under {}", indexName, databasePath.toString());
						}
					}
				} catch (IOException e) {
					throw new UncheckedIOException(new IOException(
							"Failed to merge index " + indexName + " in database " + databasePath, e));
				}
			});

			Path dataPath = DatabaseSupport.getDatabaseDataDirectoryPath(databasePath);
			Path commitDataPath = DatabaseSupport.getDatabaseDataDirectoryPath(commitDatabasePath);
			Path commitDataIndexDirectoryPath = DatabaseSupport.getDatabaseDataIndexDirectoryPath(commitDatabasePath);

			// move the file indexes for the changed files to the database
			Stream.of(keyIndexes).forEach(keyIndex -> {
				String indexName = keyIndex.getName();
				DatabaseIndex databaseIndex;
				try {
					databaseIndex = commitDatabase.getIndex(indexName);
				} catch (IOException ex) {
					throw new UncheckedIOException(new IOException(
							"Failed to load staging index file " + indexName + " in database " + commitDatabasePath,
							ex));
				}
				Stream.of(databaseIndex.getFileNames()).forEach(fileName -> {
					Path relativeDataPath = fileSystem.getPath(fileName);
					Path nextRelativeDataPath = fileSystem.getPath(fileName + "." + nextCommit);

					Path commitFileIndexPath = DatabaseSupport.getFileIndexPath(commitDatabasePath, relativeDataPath,
							indexName);
					Path fileIndexPath = DatabaseSupport.getFileIndexPath(databasePath, nextRelativeDataPath,
							indexName);

					try {
						if (Files.exists(commitFileIndexPath)) {
							Files.createDirectories(fileIndexPath.getParent());
							Files.move(commitFileIndexPath, fileIndexPath);
						}
					} catch (IOException ex) {
						throw new UncheckedIOException(
								new IOException("Failed to move staging index " + commitFileIndexPath, ex));
					}
				});
			});

			// move the changed files to the database
			Stream.of(keyIndexes).map(keyIndex -> {
				String indexName = keyIndex.getName();
				try {
					return commitDatabase.getIndex(indexName);
				} catch (IOException ex) {
					throw new UncheckedIOException(new IOException(
							"Failed to load staging index file " + indexName + " in database " + commitDatabasePath,
							ex));
				}
			}).flatMap(databaseIndex -> Stream.of(databaseIndex.getFileNames())).distinct().forEach(fileName -> {

				Path commitFilePath = commitDataPath.resolve(fileName);
				Path databaseFilePath = dataPath.resolve(fileName + "." + nextCommit);
				Path fileIndexDirectoryPath = commitDataIndexDirectoryPath.resolve(fileName);

				try {
					if (Files.exists(commitFilePath)) {
						Files.createDirectories(databaseFilePath.getParent());
						Files.move(commitFilePath, databaseFilePath);
					}
					Files.deleteIfExists(fileIndexDirectoryPath);

				} catch (IOException ex) {
					throw new UncheckedIOException(
							new IOException("Failed to move staging data file " + commitFilePath, ex));
				}
			});

			new DatabaseRemoveFilesPersister().persist(removeFilesPath, removeFilesSet);

			Path commitPath = DatabaseSupport.getDatabaseCommitPath(databasePath, nextCommit);
			Files.createFile(commitPath);

			deleteDatabase(commitDatabasePath);

			Database newDatabase = loadDatabase(databasePath, nextCommit);
			return mergeNewDatabase(database, newDatabase);

		} catch (UncheckedIOException e) {
			throw e.getCause();
		}
	}

	private Database loadDatabase(Path databasePath, long nextCommit) {
		return new Database(this, databasePath, nextCommit);
	}

	private Database mergeNewDatabase(Database currentDatabase, Database newDatabase) {

		if (currentDatabase != null) {
			currentDatabase.setNextDatabase(newDatabase);
			try {
				currentDatabase.readUnlock();
			} catch (IOException e) {
				throw new UncheckedIOException(
						new IOException("Failed to close database " + currentDatabase.getDatabasePath(), e));
			}
		}

		return newDatabase;
	}
}
