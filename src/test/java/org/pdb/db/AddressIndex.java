package org.pdb.db;

import java.nio.file.Path;

public class AddressIndex extends KeyIndex<Address>{

	@Override
	public String getName() {
		
		return "address";
	}

	@Override
	public Address fromKeyString(String keyString) {
		String[] fields = DelimitedDataSupport.fields(keyString, '|');
		
		return new Address(fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6]);
	}

	@Override
	public boolean canIndex(Path relativePath, boolean isDirectory) {
		return "address".equals(relativePath.getName(0).toString());
	}


	@Override
	public KeyIndexer<Address> getKeyIndexer(Path relativePath) {
		
		return new AddressIndexer();
	}
}
