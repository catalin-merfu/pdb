package org.pdb.db;

public class ProvinceCodeIndexer extends StringIndexer {

	@Override
	public String keyStringFromLine(String line) {
		int endProvinceCodeIdx = line.indexOf("|");
		String provinceCode =  endProvinceCodeIdx >= 0 ? line.substring(0, endProvinceCodeIdx) : line;
		return provinceCode.isBlank() ? null : provinceCode;
	}

	@Override
	public FileFormat getFileFormat() {
		
		return new StandardFileFormat();
	}
}