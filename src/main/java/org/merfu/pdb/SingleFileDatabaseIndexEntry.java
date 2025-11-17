package org.merfu.pdb;

import java.util.stream.Stream;

class SingleFileDatabaseIndexEntry extends DatabaseIndexEntry {
	
	private String fileName;

	public SingleFileDatabaseIndexEntry(String keyString, String fileName) {
		super(keyString);
		this.fileName = fileName;
	}

	@Override
	public Stream<String> getFileNames() {

		return Stream.of(fileName);
	}

	public void internFields() {
		super.internFields();
		// this.fileName = fileName.intern();
	}
	
}
