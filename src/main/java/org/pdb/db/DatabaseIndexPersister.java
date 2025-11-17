package org.pdb.db;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class DatabaseIndexPersister {

	public void persist(Path databaseIndexPath, KeyIndex<?> keyIndex,  Map<String, List<String>> keystringToPathIdListMap) throws IOException {

		if(Files.exists(databaseIndexPath))
			return;
		
		Files.createDirectories(databaseIndexPath.getParent());
		
		Path tempDatabaseIndexPath = DatabaseSupport.temporaryPath(databaseIndexPath);
		try (BufferedWriter writer = Files.newBufferedWriter(tempDatabaseIndexPath)) {

			keystringToPathIdListMap.entrySet().stream().sorted(Comparator.comparing(Map.Entry::getKey, keyIndex.getKeyStringComparator())).forEach(entry -> {
				String keyString = entry.getKey();
				List<String> fileIdList = entry.getValue();

				String formattedLine = fileIdList.stream().collect(Collectors.joining(",")) + "|" + keyString;

				try {
					writer.write(formattedLine);
					writer.newLine();
				}
				catch(IOException ex) {
					throw new UncheckedIOException(new IOException("Failed to persist index file " + tempDatabaseIndexPath, ex));
				}
			});
		}
		catch(UncheckedIOException ex) {
			throw ex.getCause();
		}
		Files.move(tempDatabaseIndexPath, databaseIndexPath);
	}
}
