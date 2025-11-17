package org.merfu.pdb;

import org.merfu.pdb.StandardFileFormat;

public class OdaFileFormat extends StandardFileFormat {

	public boolean isRecordHeaderLine(String line) {
		return line.startsWith("A|");
	}
}
