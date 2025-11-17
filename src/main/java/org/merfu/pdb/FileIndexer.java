package org.merfu.pdb;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.merfu.pdb.LineReader.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FileIndexer {

	@SuppressWarnings("unused")
	static private Logger logger = LoggerFactory.getLogger(FileIndexer.class);
	
	public Stream<FileIndex> index(Path databasePath, Path relativeFilePath, KeyIndex<?>[] keyIndexes) throws IOException {

		Path dataFilePath = DatabaseSupport.getDatabaseDataDirectoryPath(databasePath).resolve(relativeFilePath);

		Map<Boolean, List<KeyIndex<?>>> flagToLineKeyIndexersMap = Arrays.stream(keyIndexes).collect(Collectors.partitioningBy(keyIndex -> {
			return Files.isRegularFile(DatabaseSupport.getFileIndexPath(databasePath, relativeFilePath, keyIndex.getName()));

		}));

		List<KeyIndex<?>> existingKeyIndexList = flagToLineKeyIndexersMap.get(Boolean.TRUE);
		List<KeyIndex<?>> missingKeyIndexList = flagToLineKeyIndexersMap.get(Boolean.FALSE);

		Stream<FileIndex> existingFileIndexStream = existingKeyIndexList.stream().map(keyIndex -> {
			
			String indexName = keyIndex.getName();
			try {
				return new FileIndexLoader().load(databasePath, relativeFilePath, keyIndex.getName());
			} catch (IOException e) {
				String message = "Failed to load file index " + indexName + " for file " + databasePath.resolve(relativeFilePath).toString();
				throw new StreamingException(message, e);
			}
		});

		if(!missingKeyIndexList.isEmpty()) {
			
			Map<String, KeyIndexer<?>> missingKeyIndexers = missingKeyIndexList.stream().collect(Collectors.toMap(KeyIndex::getName, keyIndex -> keyIndex.getKeyIndexer(relativeFilePath)));

			Map<String, List<FileIndexEntry>> indexNameToEntriesListMap = 
					missingKeyIndexList.stream().collect(Collectors.toMap(keyIndex -> keyIndex.getName(), lineKeyIndexer -> new ArrayLinkedList<FileIndexEntry>()));

			FileFormat fileFormat = missingKeyIndexers.get(missingKeyIndexList.get(0).getName()).getFileFormat();
			try(LineReader reader = new LineReader(dataFilePath)) {

				Callback headerCallback = new Callback() {
					@Override
					public Callback nextLine(String line, long position) {
						if(line == null)
							return null;

						if(fileFormat.isHeaderLine(line))
							return this;

						if(fileFormat.isRecordHeaderLine(line)) {
							Callback recordCallback = new Callback() {

								private long recordStart = position;
								private String recordHeader = line;

								@Override
								public Callback nextLine(String line, long position) {

									Callback callback;
									boolean isDemarcation;
									boolean isRecord;
									
									if(line == null) {
										isDemarcation = true;
										isRecord = false;
										callback = null;
									}
									else if(fileFormat.isRecordHeaderLine(line)) {
										isDemarcation = true;
										isRecord = true;
										callback = this;
									}
									else if(fileFormat.isTrailerLine(line)) {
										isDemarcation = true;
										isRecord = false;
										callback = new TrailerCallback();
									}
									else {
										isDemarcation = false;
										isRecord = false;
										callback = this;
									}
									
									if(isDemarcation)
										missingKeyIndexers.entrySet().stream().forEach(entry -> {
											String keyString = entry.getValue().keyStringFromLine(recordHeader);
											if(keyString != null) {
												FileIndexEntry fileIndexEntry = new FileIndexEntry(keyString, recordStart, position);
												indexNameToEntriesListMap.get(entry.getKey()).add(fileIndexEntry);
											}
										});
									
									if(isRecord) {
										recordStart = position;
										recordHeader = line;
									}
										
									return callback;
								}
							};

							return recordCallback;
						}

						if(fileFormat.isTrailerLine(line)) {
							return new TrailerCallback();
						}

						return this;
					}
				};

				reader.readLines(headerCallback);

			} catch (IOException e) {
				String message = "Failed to read data file " + dataFilePath;
				throw new IOException(message, e);
			} catch(LineTooLong e) {
				throw new StreamingException("A line in file " + relativeFilePath.toString() + " is longer than the internal reading buffer");
			}
			
			Stream<FileIndex> missingFileIndexStream = missingKeyIndexList.stream().map(keyIndex -> {
				String indexName = keyIndex.getName();
				List<FileIndexEntry> entriesList = indexNameToEntriesListMap.get(indexName);
				FileIndexEntry[] entries = entriesList.toArray(new FileIndexEntry[entriesList.size()]);

				Arrays.sort(entries, new Comparator<>() {

					private Comparator<String> keyStringComparator = keyIndex.getKeyStringComparator();
					
					@Override
					public int compare(FileIndexEntry o1, FileIndexEntry o2) {
						
						return keyStringComparator.compare(o1.geyKeyString(), o2.geyKeyString());
					}
				});

				Path indexPath = DatabaseSupport.getFileIndexPath(databasePath, relativeFilePath, indexName);
				try {
					saveIndex(entries, indexPath);
				} catch (IOException e) {
					String message = "Failed to save index file " + indexPath.toString();
					throw new StreamingException(message, e);
				}

				return new FileIndex(databasePath, indexName, entries, relativeFilePath);
			});
			
			return Stream.concat(existingFileIndexStream, missingFileIndexStream);
		}
		else
			return existingFileIndexStream;
	}

	private void saveIndex(FileIndexEntry[] entries, Path indexPath) throws IOException {

		Files.createDirectories(indexPath.getParent());

		Path tempIndexPath = DatabaseSupport.temporaryPath(indexPath);
		try (BufferedWriter writer = Files.newBufferedWriter(tempIndexPath)) {
			try {
				for (FileIndexEntry entry : entries) {
					String line = formatFileIndexEntry(entry);
					writer.write(line);
					writer.newLine();
				}
			} catch (IOException e) {
				throw new IOException("Failed to persist index " + indexPath.toString(), e);
			}
		}
		Files.move(tempIndexPath, indexPath, StandardCopyOption.REPLACE_EXISTING);
	}

	private String formatFileIndexEntry(FileIndexEntry entry) {
		return String.join("|", 
				Long.toString(entry.getRecordStart(), Character.MAX_RADIX), 
				Long.toString(entry.getRecordEnd(), Character.MAX_RADIX), 
				entry.geyKeyString()
			);
	}

	final private static class TrailerCallback implements  Callback {

		@Override
		public Callback nextLine(String line, long position) {
			return this;
		}
	}

}
