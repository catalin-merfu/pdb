package org.merfu.pdb;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FileIndexLoader {

	static Logger logger = LoggerFactory.getLogger(FileIndexLoader.class);
	
	public FileIndex load(Path databasePath, Path relativeDataPath, String indexName) throws IOException {

		Path indexPath = DatabaseSupport.getFileIndexPath(databasePath, relativeDataPath, indexName);
		logger.debug("Start loading file index {} ", indexPath.toString());
		
		List<FileIndexEntry> entriesList = new ArrayLinkedList<>();
		try (BufferedReader reader = Files.newBufferedReader(indexPath)) {
			try {
				String line = reader.readLine();
				while (line != null) {
					FileIndexEntry fileIndexEntry = parseRecordIndexEntry(line);
					entriesList.add(fileIndexEntry);
					line = reader.readLine();
				}
			} catch (IOException e) {
				throw new IOException("Failed to read index file " + indexPath.toString(), e);
			}
		}

		FileIndexEntry[] entries = entriesList.toArray(new FileIndexEntry[entriesList.size()]);
		
		logger.debug("Completed loading file index {} ", indexPath.toString());
		
		return new FileIndex(databasePath, indexName, entries, relativeDataPath);
	}

	private FileIndexEntry parseRecordIndexEntry(String line) {

		String[] fields = line.split("\\|", 3);

		long recordStart = Long.parseLong(fields[0], Character.MAX_RADIX);
		long recordEnd = Long.parseLong(fields[1], Character.MAX_RADIX);
		String keyString = fields[2];

		return new FileIndexEntry(keyString, recordStart, recordEnd);
	}
}
