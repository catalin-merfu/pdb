package org.merfu.pdb;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

class DatabaseFileIndexPersister {

	public void persist(Path databaseFileIndexPath, Map<String, String> fileNameToIdMap) throws IOException {
		if(Files.exists(databaseFileIndexPath))
			return;

		Path tempFileIndexPath = DatabaseSupport.temporaryPath(databaseFileIndexPath);
		try (BufferedWriter writer = Files.newBufferedWriter(tempFileIndexPath)) {
			for (Map.Entry<String, String> entry : fileNameToIdMap.entrySet()) {
				String formattedLine = entry.getValue() + "|" + entry.getKey();

				try {
					writer.write(formattedLine);
					writer.newLine();
				}
				catch(IOException ex) {
					throw new IOException("Failed to persist file index " + tempFileIndexPath, ex);
				}
			}
		}
		Files.move(tempFileIndexPath, databaseFileIndexPath);
	}
}
