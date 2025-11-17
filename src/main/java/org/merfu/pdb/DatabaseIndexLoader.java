package org.merfu.pdb;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class DatabaseIndexLoader {

	public DatabaseIndex load(String indexName, Path databasePath, long version) throws IOException {

		Path fileIndexPath = DatabaseSupport.getDatabaseFileIndexPath(databasePath, indexName, version);
		Map<String,String> idToFileNameMap = new HashMap<>();
		try(BufferedReader reader = Files.newBufferedReader(fileIndexPath)) {
			String line = reader.readLine();
			while(line != null) {
				String[] fields = line.split("\\|");
				idToFileNameMap.put(fields[0], fields[1]);

				line = reader.readLine();
			}
		}

		Path databaseIndexPath = DatabaseSupport.getDatabaseIndexPath(databasePath, indexName, version);
		List<DatabaseIndexEntry> entriesList = new ArrayLinkedList<>();
		try(BufferedReader reader = Files.newBufferedReader(databaseIndexPath)) {
			String line = reader.readLine();
			while(line != null) {
				String[] fields = line.split("\\|", 2);

				String[] files = fields[0].split(",");
				String keyString = fields[1];
				
				Arrays.setAll(files, idx -> idToFileNameMap.get(files[idx]));
				
				entriesList.add(files.length > 1 ? new MultiFilesDatabaseIndexEntry(keyString, files) : new SingleFileDatabaseIndexEntry(keyString, files[0]));
				line = reader.readLine();
			}
		}

		DatabaseIndexEntry[] entries = entriesList.toArray(new DatabaseIndexEntry[entriesList.size()]);
		entriesList = null; // allow to gc

		return new DatabaseIndex(databasePath, idToFileNameMap.values().toArray(new String[idToFileNameMap.size()]), entries);
	}
}
