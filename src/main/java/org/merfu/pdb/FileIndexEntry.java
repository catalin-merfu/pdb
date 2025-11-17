package org.merfu.pdb;

class FileIndexEntry{

	private String keyString;
	private long recordStart;
	private long recordEnd;
	
	public FileIndexEntry(String keyString, long recordStart, long recordEnd) {
		this.keyString = keyString;
		this.recordStart = recordStart;
		this.recordEnd = recordEnd;
	}
	
	public String geyKeyString() {
		return keyString;
	}

	public long getRecordStart() {
		return recordStart;
	}

	public long getRecordEnd() {
		return recordEnd;
	}

	public void internFields() {
		keyString = keyString.intern();
	}
}
