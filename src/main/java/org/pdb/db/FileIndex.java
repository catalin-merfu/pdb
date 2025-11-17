package org.pdb.db;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
class FileIndex {

	static Logger logger = LoggerFactory.getLogger(FileIndex.class);
	
	private Path databasePath;
	private String indexName;
	private FileIndexEntry[] entries;
	private Path relativeDataPath;

	FileIndex(Path databasePath, String indexName, FileIndexEntry[] entries, Path relativeDataPath) {
		this.databasePath = databasePath;
		this.indexName = indexName;
		this.entries = entries;
		this.relativeDataPath = relativeDataPath;
	}

	<K extends Comparable<K>> void lookup(List<K> keys, KeyIndex<K> keyIndex, Consumer<MatchedRecord<K>> matchedRecordsSink) throws FileNotFoundException {

		logger.debug("Executing lookup in file {}", relativeDataPath);
		Path dataPath = DatabaseSupport.getDatabaseDataDirectoryPath(databasePath).resolve(relativeDataPath);

		try(FileChannel inputChannel = FileChannel.open(dataPath)) {
			Path unversionedRelativeFilePath = relativeDataPath.getFileSystem().getPath(new VersionFile(relativeDataPath).getBasename());
			int fromIndex = 0;

			for(K key: keys) {
				fromIndex = ArrayUtils.binarySearchFirst(entries, fromIndex, entries.length, key, entry -> keyIndex.fromKeyString(entry.geyKeyString()));
				FileIndexEntry fileIndexEntry = entries[fromIndex];
				String record = readRecord(inputChannel, fileIndexEntry);
				matchedRecordsSink.accept(new MatchedRecord<>(key, record, unversionedRelativeFilePath));

				String keyString = fileIndexEntry.geyKeyString();
				while(++fromIndex < entries.length) {
					fileIndexEntry = entries[fromIndex];
					if (fileIndexEntry.geyKeyString().equals(keyString)) {
						record = readRecord(inputChannel, fileIndexEntry);
						matchedRecordsSink.accept(new MatchedRecord<>(key, record, unversionedRelativeFilePath));
					}
					else
						break;
				}
			}
		}
		catch(IOException ex) {
			throw new StreamingException("Failed to read from data file " + dataPath.toString(), ex);
		}
	}

	<K extends Comparable<K>> void streamOrdered(List<K> keys, KeyIndex<K> keyIndex, Consumer<MatchedRecord<K>> matchedRecordsSink) throws FileNotFoundException {

		logger.debug("Executing streamOrdered in file {}", relativeDataPath);
		Path dataPath = DatabaseSupport.getDatabaseDataDirectoryPath(databasePath).resolve(relativeDataPath);

		try(FileChannel inputChannel = FileChannel.open(dataPath)) {
			Path unversionedRelativeFilePath = relativeDataPath.getFileSystem().getPath(new VersionFile(relativeDataPath).getBasename());

			int fromIndex = ArrayUtils.binarySearchFirst(entries, 0, entries.length, keys.get(0), entry -> keyIndex.fromKeyString(entry.geyKeyString()));
			FileIndexEntry fileIndexEntry = entries[fromIndex];
			
			for(K key: keys) {

				String record = readRecord(inputChannel, fileIndexEntry);
				matchedRecordsSink.accept(new MatchedRecord<>(key, record, unversionedRelativeFilePath));

				String keyString = fileIndexEntry.geyKeyString();
				while(++fromIndex < entries.length) {
					fileIndexEntry = entries[fromIndex];
					if (fileIndexEntry.geyKeyString().equals(keyString)) {
						record = readRecord(inputChannel, fileIndexEntry);
						matchedRecordsSink.accept(new MatchedRecord<>(key, record, unversionedRelativeFilePath));
					}
					else
						break;
				}
			}
		}
		catch(IOException ex) {
			throw new StreamingException("Failed to read from data file " + dataPath.toString(), ex);
		}
	}

	<K extends Comparable<K>> void streamReversed(List<K> keys, KeyIndex<K> keyIndex, Consumer<MatchedRecord<K>> matchedRecordsSink) throws FileNotFoundException {

		logger.debug("Executing streamReversed in file {}", relativeDataPath);
		Path dataPath = DatabaseSupport.getDatabaseDataDirectoryPath(databasePath).resolve(relativeDataPath);

		try(FileChannel inputChannel = FileChannel.open(dataPath)) {
			Path unversionedRelativeFilePath = relativeDataPath.getFileSystem().getPath(new VersionFile(relativeDataPath).getBasename());

			int fromIndex = ArrayUtils.binarySearchLast(entries, 0, entries.length, keys.get(0), entry -> keyIndex.fromKeyString(entry.geyKeyString()));
			FileIndexEntry fileIndexEntry = entries[fromIndex];
			
			for(K key: keys) {

				String record = readRecord(inputChannel, fileIndexEntry);
				matchedRecordsSink.accept(new MatchedRecord<>(key, record, unversionedRelativeFilePath));

				String keyString = fileIndexEntry.geyKeyString();
				while(--fromIndex >= 0) {
					fileIndexEntry = entries[fromIndex];
					if (fileIndexEntry.geyKeyString().equals(keyString)) {
						record = readRecord(inputChannel, fileIndexEntry);
						matchedRecordsSink.accept(new MatchedRecord<>(key, record, unversionedRelativeFilePath));
					}
					else
						break;
				}
			}
		}
		catch(IOException ex) {
			throw new StreamingException("Failed to read from data file " + dataPath.toString(), ex);
		}
	}

	private String readRecord(FileChannel inputChannel, FileIndexEntry recordIndexEntry) throws IOException {
		long recordStart = recordIndexEntry.getRecordStart();
		long recordEnd = recordIndexEntry.getRecordEnd();
		inputChannel.position(recordStart);
		ByteBuffer byteBuffer = ByteBuffer.allocate((int)(recordEnd - recordStart));
		inputChannel.read(byteBuffer);
		
		while (byteBuffer.hasRemaining())
			inputChannel.read(byteBuffer);
		
		byteBuffer.flip();
		return new String(byteBuffer.array(), StandardCharsets.UTF_8);
	}
	
	String getIndexName() {
		return indexName;
	}

	FileIndexEntry[] getEntries() {
		return entries;
	}

	Path getDataFilePath() {
		return relativeDataPath;
	}
	
	FileIndex internFields() {
		
		DatabaseSupport.executeIntern(new Runnable() {
			
			@Override
			public void run() {
				Stream.of(entries).forEach(FileIndexEntry::internFields);
				
			}
		});
		
		return this;
	}
}
