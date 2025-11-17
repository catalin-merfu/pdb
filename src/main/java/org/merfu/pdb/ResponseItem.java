package org.merfu.pdb;

import java.nio.file.Path;

/**
 * Result record returned by a lookup function
 * 
 * @param <R> The type of the client request data
 */
public class ResponseItem<R> {

	private R referenceData;
	private String record;
	private Path dataFilePath;
	
	ResponseItem(R referenceData, String record, Path dataFilePath) {
		super();
		this.referenceData = referenceData;
		this.record = record;
		this.dataFilePath = dataFilePath;
	}

	/**
	 * 
	 * @return The client request data
	 */
	public R getReferenceData() {
		return referenceData;
	}

	/**
	 * 
	 * @return The record data or null if not found
	 */
	public String getRecord() {
		return record;
	}

	/**
	 * 
	 * @return The relative file path where the record was found or null if not found
	 */
	public Path getDataFilePath() {
		return dataFilePath;
	}
}
