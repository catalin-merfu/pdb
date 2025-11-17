package org.pdb.db;

/**
 * Indexers for string key types must subclass this class.
 */
abstract public class StringIndexer implements KeyIndexer<String> {
	
	@Override
	public String keyStringFromLine(String line) {
		return keyStringFromLine(line);
	}
}
