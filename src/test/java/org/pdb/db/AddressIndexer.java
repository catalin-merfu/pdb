package org.pdb.db;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AddressIndexer implements KeyIndexer<Address> {
	
	@Override
	public String keyStringFromLine(String line) {
		String[] fields = DelimitedDataSupport.fields(line, '|');
		
		return Stream.of(
				fields[9],
				fields[8],
				fields[3],
				fields[4],
				fields[5],
				fields[2],
				fields[6]
			).collect(Collectors.joining("|"));
	}
	
	@Override
	public FileFormat getFileFormat() {
		return new OdaFileFormat();
	}
}
