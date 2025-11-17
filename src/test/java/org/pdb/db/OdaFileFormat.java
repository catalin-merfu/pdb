package org.pdb.db;

public class OdaFileFormat extends StandardFileFormat {

	public boolean isRecordHeaderLine(String line) {
		return line.startsWith("A|");
	}
}
