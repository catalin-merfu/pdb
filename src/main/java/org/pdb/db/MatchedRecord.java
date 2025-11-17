package org.pdb.db;

import java.nio.file.Path;

/**
 * Result record returned by a database lookup
 * 
 * @param <K> The key type
 */
public class MatchedRecord<K extends Comparable<K>> {

	private K key;
	private String record;
	private Path dataFilePath;
	
	MatchedRecord(K key, String record, Path dataFilePath) {
		super();
		this.key = key;
		this.record = record;
		this.dataFilePath = dataFilePath;
	}

	/**
	 * @return The key value in the request
	 */
	public K getKey() {
		return key;
	}

	/**
	 * @return The record lines from the file or null if the key was not found
	 */
	public String getRecord() {
		return record;
	}

	/**
	 * @return The relative path of the file where the record was found or null if not found
	 */
	public Path getDataFilePath() {
		return dataFilePath;
	}
}
