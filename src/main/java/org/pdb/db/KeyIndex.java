package org.pdb.db;

import java.nio.file.Path;
import java.util.Comparator;

/**
 * A database index extracts the key from file records, formats the key for persistence and provides the index ordering of the database records.
 * 
 * @param <K> The index key type
 */
abstract public class KeyIndex<K extends Comparable<K>> {

	/**
	 * Returns the name of this index that must be provided when searching a database, see {@link Database#lookup}
	 * 
	 * @return The index name
	 */
	abstract public String getName();

	/**
	 * Parses the string representation of a key value.
	 * 
	 * @param keyString The string representation of a key
	 * @return The key value
	 */
	abstract public K fromKeyString(String keyString);

	/**
	 * Returns a comparator used for searching and sorting the string representation of the keys in index order 
	 * 
	 * @return
	 */
	Comparator<String> getKeyStringComparator() {
		
		return new Comparator<String>() {

			private String ks1;
			private K k1;
			private String ks2;
			private K k2;
			
			@Override
			public int compare(String o1, String o2) {
			
				if(o1 != ks1) {
					ks1 = o1;
					k1 = fromKeyString(o1);
				}

				if(o2 != ks2) {
					ks2 = o2;
					k2 = fromKeyString(o2);
				}

				return k1.compareTo(k2);
			}
		};
	}

	/**
	 * Filters the directories or files that should be indexed by this index. If a directory is filtered out from indexing
	 * none of the subdirectories or files under the directory structure are tested or indexed.
	 * 
	 * @param relativePath The relative path of the directory or file
	 * @param isDirectory If the relative path is a directory
	 * @return Returns true if the file or the files under the directory should be indexed
	 */
	abstract public boolean canIndex(Path relativePath, boolean isDirectory);
	
	/**
	 * Returns the key indexer for the provided database relative path.
	 * 
	 * @param relativePath The database relative path
	 * @return The key indexer for the path
	 */
	abstract public KeyIndexer<K> getKeyIndexer(Path relativePath);
}
