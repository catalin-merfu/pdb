package org.merfu.pdb;

import java.nio.file.Path;

import org.merfu.pdb.KeyIndexer;
import org.merfu.pdb.StringIndex;

public class ProvinceCodeIndex extends StringIndex {

	@Override
	public String getName() {
		
		return "provinceCode";
	}

	@Override
	public boolean canIndex(Path relativePath, boolean isDirectory) {
		return "province".equals(relativePath.getName(0).toString());
	}
	
	@Override
	public KeyIndexer<String> getKeyIndexer(Path relativePath) {
		return new ProvinceCodeIndexer();
	}
}
