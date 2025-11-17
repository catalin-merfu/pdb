package org.merfu.pdb;

import java.util.stream.Stream;

class MultiFilesDatabaseIndexEntry extends DatabaseIndexEntry {

	private String[] fileNames;

	public MultiFilesDatabaseIndexEntry(String keyString, String[] fileNames) {
		super(keyString);
		this.fileNames = fileNames;
	}

	public Stream<String> getFileNames() {
		return Stream.of(fileNames);
	}
	
	public void internFields() {
		super.internFields();
		// Arrays.setAll(fileNames, idx -> fileNames[idx].intern());
	}
}
