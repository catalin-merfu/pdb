package org.pdb.db;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class DatabaseIndex {

	private static int ORDERED_LOOKUP_WINDOW = 1024;

	private Path databasePath;
	private Path databaseDataPath;

	private String[] fileNames;
	private DatabaseIndexEntry[] entries;

	private Map<String, FileIndex> fileNameToFileIndexMap;

	public DatabaseIndex(Path databasePath, String[] fileNames, DatabaseIndexEntry[] entries) {
		this.databasePath = databasePath;
		databaseDataPath = DatabaseSupport.getDatabaseDataDirectoryPath(databasePath);

		this.fileNames = fileNames;
		this.entries = entries;

		fileNameToFileIndexMap = DatabaseSupport.softValuesMap();
	}

	<K extends Comparable<K>> Stream<MatchedRecord<K>> lookup(Stream<K> keys, KeyIndex<K> keyIndex) throws IOException {

		List<K> nonMatchedKeys = new ArrayLinkedList<>();
		
		Stream<SimpleImmutableEntry<K, Stream<String>>> keyEntries = keys.mapMulti(new BiConsumer<K, Consumer<SimpleImmutableEntry<K, Stream<String>>>>() {

				private K lastKey;
				private int fromIndex;

				@Override
				public void accept(K key,
						Consumer<SimpleImmutableEntry<K, Stream<String>>> databaseIndexEntrySink) {
					boolean filterOut = key.equals(lastKey);
					lastKey = key;
					if (filterOut)
						return;

					int recordIndex = ArrayUtils.binarySearch(entries, fromIndex, entries.length, key,
							databaseIndexEntry -> keyIndex.fromKeyString(databaseIndexEntry.getKeyString()));
					if (recordIndex < 0) {
						nonMatchedKeys.add(key);
						return;
					} else {
						fromIndex = recordIndex;
						databaseIndexEntrySink
								.accept(new SimpleImmutableEntry<>(key, entries[fromIndex++].getFileNames()));
					}
				}
			});

		Map<String, List<K>> fileNameToKeyListMap = fileNameToKeyListMap(keyEntries);		
		Stream<MatchedRecord<K>> matchedRecords = lookupFiles(keyIndex, fileNameToKeyListMap);

		Stream<MatchedRecord<K>> nonMatchedRecords = nonMatchedKeys.stream()
			.map(key -> new MatchedRecord<>(key, null, null));

		return Stream.concat(nonMatchedRecords.parallel(), matchedRecords);
	}

	<K extends Comparable<K>> Stream<MatchedRecord<K>> streamOrdered(KeyIndex<K> keyIndex) throws IOException {
		return streamOrdered(keyIndex, 0);
	}

	<K extends Comparable<K>> Stream<MatchedRecord<K>> streamOrdered(KeyIndex<K> keyIndex, K greaterThan) throws IOException {
		
		int recordIndex = ArrayUtils.binarySearch(entries, 0, entries.length, greaterThan,
				databaseIndexEntry -> keyIndex.fromKeyString(databaseIndexEntry.getKeyString()));
		
		if(recordIndex < 0)
			recordIndex = -recordIndex - 1;
		else
			recordIndex++;

		return streamOrdered(keyIndex, recordIndex);
	}

	private <K extends Comparable<K>> Stream<MatchedRecord<K>> streamOrdered(KeyIndex<K> keyIndex, int startIndex) throws IOException {

		return Stream.generate(new Supplier<Integer>() {

			int startWindow = startIndex;
			
			@Override
			public Integer get() {
				
				int current = startWindow;
				startWindow += ORDERED_LOOKUP_WINDOW;
				return current;
			}
		}).takeWhile(startWindow -> startWindow < entries.length).map(new Function<Integer, Stream<SimpleImmutableEntry<K, Stream<String>>>>() {

			@Override
			public Stream<SimpleImmutableEntry<K, Stream<String>>> apply(Integer startWindow) {

				return Stream.generate(new Supplier<SimpleImmutableEntry<K, Stream<String>>>() {

					int current = startWindow;
					
					@Override
					public SimpleImmutableEntry<K, Stream<String>> get() {
						DatabaseIndexEntry databaseIndexEntry = entries[current++];
						
						K key = keyIndex.fromKeyString(databaseIndexEntry.getKeyString());
						return new SimpleImmutableEntry<>(key, databaseIndexEntry.getFileNames());
					}
				}).limit(Math.min(startWindow + ORDERED_LOOKUP_WINDOW, entries.length) - startWindow);
			}
		}).map(keyEntries -> {
			Map<String, List<K>> fileNameToKeyListMap = fileNameToKeyListMap(keyEntries);		
			Stream<MatchedRecord<K>> matchedRecords = lookupFilesOrdered(keyIndex, fileNameToKeyListMap);
			if(fileNameToKeyListMap.size() > 1)
				matchedRecords = matchedRecords.sorted(Comparator.comparing(matchedRecord -> matchedRecord.getKey()));
			
			return matchedRecords;
		}).flatMap(Function.identity());
	}

	<K extends Comparable<K>> Stream<MatchedRecord<K>> streamReversed(KeyIndex<K> keyIndex) throws IOException {
		return streamReversed(keyIndex, entries.length);
	}

	<K extends Comparable<K>> Stream<MatchedRecord<K>> streamReversed(KeyIndex<K> keyIndex, K lesserThan) throws IOException {
		
		int recordIndex = ArrayUtils.binarySearch(entries, 0, entries.length, lesserThan,
				databaseIndexEntry -> keyIndex.fromKeyString(databaseIndexEntry.getKeyString()));
		
		if(recordIndex < 0)
			recordIndex = -recordIndex - 1;

		return streamReversed(keyIndex, recordIndex);
	}
	
	private <K extends Comparable<K>> Stream<MatchedRecord<K>> streamReversed(KeyIndex<K> keyIndex, int startIndex) throws IOException {

		return Stream.generate(new Supplier<Integer>() {

			int startWindow = startIndex;
			
			@Override
			public Integer get() {
				
				int current = startWindow;
				startWindow -= ORDERED_LOOKUP_WINDOW;
				return current;
			}
		}).takeWhile(startWindow -> startWindow > 0).map(new Function<Integer, Stream<SimpleImmutableEntry<K, Stream<String>>>>() {

					@Override
					public Stream<SimpleImmutableEntry<K, Stream<String>>> apply(Integer startWindow) {

						return Stream.generate(new Supplier<SimpleImmutableEntry<K, Stream<String>>>() {

							int current = startWindow;
							
							@Override
							public SimpleImmutableEntry<K, Stream<String>> get() {
								DatabaseIndexEntry databaseIndexEntry = entries[--current];
								
								K key = keyIndex.fromKeyString(databaseIndexEntry.getKeyString());
								return new SimpleImmutableEntry<>(key, databaseIndexEntry.getFileNames());
							}
						}).limit(startWindow - Math.max(startWindow - ORDERED_LOOKUP_WINDOW, 0));
					}
		}).map(keyEntries -> {
			Map<String, List<K>> fileNameToKeyListMap = fileNameToKeyListMap(keyEntries);		
			Stream<MatchedRecord<K>> matchedRecords = lookupFilesReversed(keyIndex, fileNameToKeyListMap);
			if(fileNameToKeyListMap.size() == 1)
				return matchedRecords;
			else
				return matchedRecords.sorted(Comparator.<MatchedRecord<K>, K>comparing(matchedRecord -> matchedRecord.getKey()).reversed());
		}).flatMap(Function.identity());
	}

	private <K extends Comparable<K>> Map<String, List<K>> fileNameToKeyListMap(Stream<SimpleImmutableEntry<K, Stream<String>>> keyEntries) {
		
		return keyEntries.flatMap(entry -> {

			K key = entry.getKey();
			Stream<String> fileNames = entry.getValue();

			return fileNames.map(fileName -> new SimpleImmutableEntry<>(fileName, key));
		})
		.collect(Collectors.groupingBy(entry -> entry.getKey(),
				Collectors.mapping(entry -> entry.getValue(), ArrayLinkedList.collector())));
	}
	
	private <K extends Comparable<K>> Stream<MatchedRecord<K>> lookupFiles(KeyIndex<K> keyIndex, Map<String, List<K>> fileNameToKeyListMap) {
		
		return keysForFileIndexStream(keyIndex, fileNameToKeyListMap).<MatchedRecord<K>>mapMulti((entry, matchedRecordsSink) -> {

			FileIndex fileIndex = entry.getKey();
			List<K> fileKeys = entry.getValue();

			try {
				fileIndex.lookup(fileKeys, keyIndex, matchedRecordsSink);
			} catch (FileNotFoundException e) {
				throw new StreamingException(e);
			}
		});
	}

	private <K extends Comparable<K>> Stream<MatchedRecord<K>> lookupFilesOrdered(KeyIndex<K> keyIndex, Map<String, List<K>> fileNameToKeyListMap) {
		
		return keysForFileIndexStream(keyIndex, fileNameToKeyListMap).<MatchedRecord<K>>mapMulti((entry, matchedRecordsSink) -> {

			FileIndex fileIndex = entry.getKey();
			List<K> fileKeys = entry.getValue();

			try {
				fileIndex.streamOrdered(fileKeys, keyIndex, matchedRecordsSink);
			} catch (FileNotFoundException e) {
				throw new StreamingException(e);
			}
		});
	}

	private <K extends Comparable<K>> Stream<MatchedRecord<K>> lookupFilesReversed(KeyIndex<K> keyIndex, Map<String, List<K>> fileNameToKeyListMap) {
		
		return keysForFileIndexStream(keyIndex, fileNameToKeyListMap).<MatchedRecord<K>>mapMulti((entry, matchedRecordsSink) -> {

			FileIndex fileIndex = entry.getKey();
			List<K> fileKeys = entry.getValue();

			try {
				fileIndex.streamReversed(fileKeys, keyIndex, matchedRecordsSink);
			} catch (FileNotFoundException e) {
				throw new StreamingException(e);
			}
		});
	}

	private <K extends Comparable<K>> Stream<SimpleImmutableEntry<FileIndex, List<K>>> keysForFileIndexStream(KeyIndex<K> keyIndex, Map<String, List<K>> fileNameToKeyListMap) {
		
		FileSystem fileSystem = databasePath.getFileSystem();
		return fileNameToKeyListMap.entrySet().stream().map(entry -> {
			String indexFilePath = entry.getKey();
			List<K> fileKeys = entry.getValue();

			FileIndex fileIndex = fileNameToFileIndexMap.computeIfAbsent(indexFilePath, name -> {
				Path relativeDataPath = fileSystem.getPath(name);

				try {
					return new FileIndexLoader().load(databasePath, relativeDataPath, keyIndex.getName())
							.internFields();
				} catch (IOException e) {
					throw new StreamingException("Failed to load file index "
							+ databaseDataPath.resolve(relativeDataPath).toString(), e);
				}
			});
			
			return new SimpleImmutableEntry<>(fileIndex, fileKeys);
		});
	}
	
	public String[] getFileNames() {
		return fileNames;
	}

	public DatabaseIndexEntry[] getEntries() {
		return entries;
	}

	public DatabaseIndex internFields() {

		DatabaseSupport.executeIntern(new Runnable() {

			@Override
			public void run() {
				Stream.of(entries).forEach(DatabaseIndexEntry::internFields);
				Arrays.setAll(fileNames, idx -> fileNames[idx].intern());
			}
		});

		return this;
	}
}
