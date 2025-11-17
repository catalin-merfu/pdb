package org.pdb.db;

import java.util.stream.Stream;

abstract class DatabaseIndexEntry {

	private String keyString;

	public DatabaseIndexEntry(String keyString) {
		this.keyString = keyString;
	}
	
	public String getKeyString() {
		return keyString;
	}

	abstract public Stream<String> getFileNames();
	
	public void internFields() {
		keyString = keyString.intern();
	}
}
