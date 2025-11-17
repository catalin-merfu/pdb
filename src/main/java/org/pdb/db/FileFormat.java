package org.pdb.db;

/**
 * Describes the file format for a database file
 */
public interface FileFormat {

	/**
	 * Determines if the line is a header line, by default if it starts with 'H|'
	 * 
	 * @param line Line in the database file parsed by the indexer
	 * @return True if this line is a header line
	 */
	default public boolean isHeaderLine(String line) {
		return line.startsWith("H|");
	}
	
	/**
	 * Determines if the line is a record header line. By default each line other than the header and trailer 
	 * is a record and the method returns true. 
	 * 
	 * For multiline records the first line is the record header
	 * and the method must return true. The lines following the record header are detailed record and the method must return false
	 * 
	 * @param line Line in the database file parsed by the indexer
	 * @return True if this line is the first line of a record
	 */
	default public boolean isRecordHeaderLine(String line) {
		return true;
	}

	/**
	 * Determines if the line is a trailer line, by default if it starts with 'T|'
	 * 
	 * @param line Line in the database file parsed by the indexer
	 * @return True if this line is a trailer line
	 */
	default public boolean isTrailerLine(String line) {
		return line.startsWith("T|");
	}
}
