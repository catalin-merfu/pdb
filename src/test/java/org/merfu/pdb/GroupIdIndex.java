package org.merfu.pdb;

import java.nio.file.Path;

import org.merfu.pdb.KeyIndexer;
import org.merfu.pdb.StringIndex;

public class GroupIdIndex extends StringIndex {

	@Override
	public String getName() {
		
		return "groupId";
	}

	@Override
	public boolean canIndex(Path relativePath, boolean isDirectory) {
		return "address".equals(relativePath.getName(0).toString());
	}

	@Override
	public KeyIndexer<String> getKeyIndexer(Path relativePath) {
		
		return new GroupIdIndexer();
	}

}
