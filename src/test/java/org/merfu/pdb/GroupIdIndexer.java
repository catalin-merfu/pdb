package org.merfu.pdb;

import org.merfu.pdb.FileFormat;
import org.merfu.pdb.StringIndexer;

public class GroupIdIndexer extends StringIndexer {

	@Override
	public String keyStringFromLine(String line) {
		char fieldSeparator = '|';
		int endRecordTypeIdx = line.indexOf(fieldSeparator);
		int startGroupIdIdx = endRecordTypeIdx + 1;
		int endGroupidIdx = line.indexOf(fieldSeparator, startGroupIdIdx);
		String groupId =  endGroupidIdx >= 0 ? line.substring(startGroupIdIdx, endGroupidIdx) : line;
		return groupId.isBlank() ? null : groupId;
	}

	@Override
	public FileFormat getFileFormat() {
		
		return new OdaFileFormat();
	}
}
