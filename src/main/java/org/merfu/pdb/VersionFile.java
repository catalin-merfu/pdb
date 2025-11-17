package org.merfu.pdb;

import java.nio.file.Path;

class VersionFile {

	private Path path;
	private String basename;
	private Long version;
	
	public VersionFile(Path path) {
		
		this.path = path;
		String filename = path.toString();

		int lastDotIdx = filename.lastIndexOf('.');
		if(lastDotIdx >= 0) {
			try {
				version = Long.parseLong(filename.substring(lastDotIdx + 1));
				basename = filename.substring(0, lastDotIdx);
			}
			catch(NumberFormatException e) {
				version = -1L;
				basename = filename;
			}
		}
		else {
			version = -1L;
			basename = filename;
		}
	}
	
	public Path getPath() {
		return path;
	}

	public String getBasename() {
		return basename;
	}

	public long getVersion() {
		return version;
	}
}
