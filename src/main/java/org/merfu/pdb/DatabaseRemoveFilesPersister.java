package org.merfu.pdb;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

class DatabaseRemoveFilesPersister {

	public void persist(Path removeFilesPath, Set<String> removeFilesSet) throws IOException {

		if(Files.exists(removeFilesPath))
			return;
		
		Files.createDirectories(removeFilesPath.getParent());

		Path tempRemoveFilesPath = DatabaseSupport.temporaryPath(removeFilesPath);
		try (BufferedWriter writer = Files.newBufferedWriter(tempRemoveFilesPath)) {
			removeFilesSet.stream().forEach(fileName -> {

				try {
					writer.write(fileName);
					writer.newLine();
				}
				catch(IOException ex) {
					throw new UncheckedIOException(new IOException("Failed to persist index file " + tempRemoveFilesPath, ex));
				}
			});
		}
		catch(UncheckedIOException ex) {
			throw ex.getCause();
		}
		Files.move(tempRemoveFilesPath, removeFilesPath);
	}
}
