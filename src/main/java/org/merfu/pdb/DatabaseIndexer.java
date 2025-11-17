package org.merfu.pdb;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DatabaseIndexer {

	static private Logger logger = LoggerFactory.getLogger(DatabaseIndexer.class);
	
	private Pdb pdb;
	private Path databasePath;
	private Path databaseDataPath;
	private long commit;
	
	public DatabaseIndexer(Pdb pdb, Path databasePath, long commit) {
		this.pdb = pdb;
		this.databasePath = databasePath;
		this.databaseDataPath = DatabaseSupport.getDatabaseDataDirectoryPath(databasePath);
		this.commit = commit;
	}

	public void index() throws IOException {

		try {
			KeyIndex<?>[] missingKeyIndexes = Arrays.stream(pdb.keyIndexes).filter(keyIndex -> {
				String indexName = keyIndex.getName();
				
				Path indexPath = DatabaseSupport.getDatabaseIndexPath(databasePath, indexName, commit);
				Path fileIndexPath = DatabaseSupport.getDatabaseFileIndexPath(databasePath, indexName, commit);
				
				return !(Files.isRegularFile(indexPath) && Files.isRegularFile(fileIndexPath));
			}).toList().toArray(new KeyIndex<?>[0]);
			
			Stream<ListResults> filePathStream = listDataDirectory(databaseDataPath.relativize(databaseDataPath), missingKeyIndexes);

			logger.debug("Starting indexing files under directory {}...", databasePath.toString());
			Stream<FileIndex> fileIndexStream = filePathStream.flatMap(listResult -> {
	
				Path relativeFilePath = listResult.getPath();
				try {
					return new FileIndexer().index(databasePath, relativeFilePath, listResult.getKeyIndexes());
				} catch (IOException e) {
					String message = "Failed to index file " + databasePath.resolve(relativeFilePath).toString();
					throw new UncheckedIOException(new IOException(message, e));
				}
			});

			Map<String, List<FileIndex>> indexNameToFileIndexMap = fileIndexStream.collect(Collectors.groupingBy(fileIndex -> fileIndex.getIndexName(), ArrayLinkedList.collector()));
			logger.debug("Completed indexing files under directory {}", databasePath.toString());
			
			Arrays.stream(missingKeyIndexes).forEach(
				keyIndex -> {
					String indexName = keyIndex.getName();
					List<FileIndex> namefileIndexList = indexNameToFileIndexMap.get(indexName);
					Stream<FileIndex> namefileIndexStream =
							namefileIndexList == null ? new SinglyLinkedList<FileIndex>().stream() : namefileIndexList.stream();

					Map<String, String> fileNameToIdMap = new HashMap<>();

					logger.debug("Starting merging file indexes for index '{}' under '{}'...", indexName, databasePath.toString());
					Map<String, List<String>> keystringToPathIdListMap = namefileIndexStream.flatMap(new Function<FileIndex, Stream<SimpleImmutableEntry<String, String>>>() {
						
						private int nextFileId;
						
						public Stream<SimpleImmutableEntry<String, String>> apply(FileIndex fileIndex) {
							FileIndexEntry[] entries = fileIndex.getEntries();
							String fileDataPath = fileIndex.getDataFilePath().toString();
							String dataFileId = fileNameToIdMap.computeIfAbsent(fileDataPath, fileName ->  Integer.toString(++nextFileId, Character.MAX_RADIX));
	
							return Arrays.stream(entries).map(entry -> entry.geyKeyString()).filter(new Predicate<String>() {
								private String previousValue;
	
								@Override
								public boolean test(String current) {
									boolean isEqual = !current.equals(previousValue);
									previousValue = current;
	
									return isEqual;
								}
							}).map(keyString -> new SimpleImmutableEntry<>(keyString, dataFileId));
						}
					}).collect(Collectors.groupingBy(entry -> entry.getKey(),
							Collectors.mapping(entry -> entry.getValue(), SinglyLinkedList.collector())));
					logger.debug("Completed merging file indexes for index '{}' under '{}'", indexName, databasePath.toString());
					
					try {
						Path indexPath = DatabaseSupport.getDatabaseIndexPath(databasePath, indexName, commit);
						Path fileIndexPath = DatabaseSupport.getDatabaseFileIndexPath(databasePath, indexName, commit);
						
						logger.debug("Starting saving database index '{}' under '{}'...", indexName, databasePath.toString());
						new DatabaseIndexPersister().persist(indexPath, keyIndex, keystringToPathIdListMap);
						new DatabaseFileIndexPersister().persist(fileIndexPath, fileNameToIdMap);
						logger.debug("Completed saving database index '{}' under '{}'", indexName, databasePath.toString());
					}
					catch(IOException e) {
						String message = "Failed to index " + indexName + " in database " + databasePath.toString();
						throw new UncheckedIOException(new IOException(message, e));
					}
				}
			);
		}
		catch(UncheckedIOException ex) {
			throw ex.getCause();
		}
	}

	private Stream<ListResults> listDataDirectory(Path dirPath, KeyIndex<?>[] keyIndexes) throws IOException {
		return Stream.concat(listDataSubdirs(dirPath, keyIndexes), listDataFiles(dirPath, keyIndexes));
	}
	
	private Stream<ListResults> listDataSubdirs(Path dirPath, KeyIndex<?>[] keyIndexes) throws IOException {
		return filterLineKeyIndexers(listDataDir(dirPath, Files::isDirectory), keyIndexes, true).flatMap(listResult -> {
			try {
				Path subdirPath = listResult.getPath();
				KeyIndex<?>[] subdirLineKeyIndexers = listResult.getKeyIndexes();
				
				return listDataDirectory(subdirPath, subdirLineKeyIndexers);
			} catch (IOException e) {
				String message = "Failed to index directory " + listResult.getPath().toString();
				throw new StreamingException(new IOException(message, e));
			}
		});
	}

	private Stream<ListResults> listDataFiles(Path dirPath, KeyIndex<?>[] keyIndexes) throws IOException {

		Stream<Path> paths = listDataDir(dirPath, Files::isRegularFile);

		return filterLineKeyIndexers(paths, keyIndexes, false);
	}

	private Stream<Path> listDataDir(Path relativeDataDirPath, Predicate<Path> filter) throws IOException {
		
		try {
			return Stream.of(databaseDataPath.resolve(relativeDataDirPath)).<Path>mapMulti((path, consumer) -> {
				try(Stream<Path> dirStream = Files.list(path)) {
					
					dirStream.filter(filter).forEach(dirEntry -> consumer.accept(dirEntry));
				} catch (IOException e) {
					throw new UncheckedIOException(new IOException("Failed to list directory " + path.toString(), e));
				}
			});
		}
		catch(UncheckedIOException e) {
			throw e.getCause();
		}
	}
	
	private Stream<ListResults> filterLineKeyIndexers(Stream<Path> pathStream, KeyIndex<?>[] keyIndexes, boolean isDirectory) {
		return pathStream.map(databaseDataPath::relativize).map(path -> {
			KeyIndex<?>[] filteredKeyIndexes= Arrays.stream(keyIndexes).filter(keyIndex -> keyIndex.canIndex(path, isDirectory)).toArray(KeyIndex<?>[]::new);
			return new ListResults(path, filteredKeyIndexes);
		}).filter(listResult -> listResult.getKeyIndexes().length != 0);
	}

	/**
	 * 
	 */
	private static class ListResults {
		
		private Path path;
		private KeyIndex<?>[] keyIndexes;
		
		public ListResults(Path path, KeyIndex<?>[] keyIndexes) {
			
			this.path= path;
			this.keyIndexes = keyIndexes;
		}

		public Path getPath() {
			return path;
		}

		public KeyIndex<?>[] getKeyIndexes() {
			return keyIndexes;
		}
	}
}
