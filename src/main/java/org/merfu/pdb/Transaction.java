package org.merfu.pdb;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Iterator;

/**
 * A transaction is used to add or update files in a database.
 */
public class Transaction {
	
	private Pdb pdb;
	private Database database;
	private Path txPath;
	private Path txDataPath;

	private boolean committed;
	private boolean rolledBack;
	private int writersCount;
	
	Transaction(Pdb pdb, Database database, long transactionId) throws IOException {

		this.pdb = pdb;
		this.database = database;
		
		txPath = DatabaseSupport.getDatabaseTxDirectoryPath(database.getDatabasePath()).resolve(Long.toString(transactionId));
		txDataPath = DatabaseSupport.getDatabaseDataDirectoryPath(txPath);
		Files.createDirectories(txDataPath);
		
		writersCount = 1;
		
		database.readLock();
	}

	synchronized int writeLock() {
		if(writersCount == 0 || committed)
			return 0;
		
		return ++writersCount;
	}

	synchronized int writeUnlock() throws IOException {

		if(writersCount == 0)
			return 0;

		if(--writersCount == 0) {
			try {
				if(committed)
					executeCommit();
				else
					executeRollback();
			}
			finally {
				database.readUnlock();
			}
		}

		return writersCount;
	}

	/**
	 * Adds or updates a database file.
	 * 
	 * @param sourcePath The source of the file on the local file system
	 * @param relativeDestinationPath The path in the database where the file is copied 
	 * @param link Copy or hard link 
	 * @throws IOException if an I/O error occurs when reading or writing the file system
	 */
	public void copyFile(Path sourcePath, Path relativeDestinationPath, boolean link) throws IOException {
		try {
			if(writeLock() == 0)
				return;

			Path destinationPath = txDataPath.resolve(relativeDestinationPath);
			Files.createDirectories(destinationPath.getParent());

			if(link)
				Files.createLink(destinationPath, sourcePath);
			else
				Files.copy(sourcePath, destinationPath);
		}
		finally {
			writeUnlock();
		}
	}

	/**
	 * Adds or updates a database file.
	 * 
	 * @param input The file data
	 * @param relativeDestinationPath The path in the database where the file data is copied 
	 * @throws IOException if an I/O error occurs when reading or writing the file system
	 */
	public void copyInputStream(InputStream input, Path relativeDestinationPath) throws IOException {
		try {
			if(writeLock() == 0)
				return;

			Path destinationPath = txDataPath.resolve(relativeDestinationPath);
			Files.createDirectories(destinationPath.getParent());

			Files.copy(input, destinationPath);
		}
		finally {
			writeUnlock();
		}
	}

	/**
	 * Adds or updates files provided under a database directory on the local files system
	 * 
	 * @param sourcePath The source directory root
	 * @param relativeDestinationPath The relative directory path where the database files are created
	 * @param link Copy or hard link
	 * @throws IOException if an I/O error occurs when reading or writing the file system
	 */
	public void copyDir(Path sourcePath, Path relativeDestinationPath, boolean link) throws IOException {
		
		if(!Files.isDirectory(sourcePath))
			throw new IOException("Path is not a directory: " + sourcePath.toString());

		try {
			if(writeLock() == 0)
				return;

			Path rootDestinationPath = (relativeDestinationPath == null ? txDataPath : txDataPath.resolve(relativeDestinationPath));
			Files.walkFileTree(sourcePath, new SimpleFileVisitor<Path>() {
	
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					Path destinationPath = resolveSourceFileSystemPath(rootDestinationPath, sourcePath.relativize(file));
					
					Files.createDirectories(destinationPath.getParent());

					if(link)
						Files.createLink(destinationPath, file);
					else
						Files.copy(file, destinationPath);

					return FileVisitResult.CONTINUE;
				}
			});
		}
		finally {
			writeUnlock();
		}
	}

	private Path resolveSourceFileSystemPath(Path destinationPath, Path sourcePath) {
		Iterator<Path> iter = sourcePath.iterator();
		while(iter.hasNext()) {
			destinationPath = destinationPath.resolve(iter.next().toString());
		}
		
		return destinationPath;
	}

	/**
	 * Commits this transaction merging the transaction files with the existing database data
	 * 
	 * @throws IOException if an I/O error occurs when reading or writing the file system
	 */
	synchronized public void commit() throws IOException {
		if(committed || rolledBack)
			return;
		else {
			Path committedPath = DatabaseSupport.getDatabaseCommittedPath(txPath);
			if(!Files.isRegularFile(committedPath))
				Files.createFile(DatabaseSupport.getDatabaseCommittedPath(txPath));
			committed = true;
			writeUnlock();
		}
	}

	/**
	 * Removes the temporary data created for this transaction
	 *  
	 * @throws IOException if an I/O error occurs when reading or writing the file system
	 */
	synchronized public void rollback() throws IOException {
		if(committed || rolledBack)
			return;
		else {
			rolledBack = true;
			writeUnlock();
		}
	}

	private void executeRollback() throws IOException {
		pdb.rollbackTransaction(txPath);
	}

	void executeCommit() throws IOException {
		new DatabaseIndexer(pdb, txPath, 0).index();

		try {
			Path databasePath = database.getDatabasePath();
			String commitDatabaseName = databasePath.getFileName().toString();
			pdb.commitTransaction(commitDatabaseName, databasePath, txPath);
		}
		catch(IOException e) {
			executeRollback();
			throw e;
		}
	}
}
