package org.pdb.db;

/**
 * A key indexer extracts the key values from the database files.
 * 
 * @param <K> The key type
 */
public interface KeyIndexer<K extends Comparable<K>> {

	/**
	 * @param line Data record header line
	 * @return The formatted string for the record key
	 */
	public String keyStringFromLine(String line);

	/**
	 * Returns the file format expected by this indexer.
	 * 
	 * @return The file format this indexer is able to parse
	 */
	public FileFormat getFileFormat();
}
