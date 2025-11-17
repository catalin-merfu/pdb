package org.pdb.db;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.OptionalLong;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections4.map.ConcurrentReferenceHashMap;

class DatabaseSupport {

	private static ThreadPoolExecutor internExecutorService;

	static {
		int poolSize = (Runtime.getRuntime().availableProcessors() + 4 - 1) / 4;
		internExecutorService = new ThreadPoolExecutor(poolSize, poolSize, 1, TimeUnit.MINUTES,
				new SynchronousQueue<>(), new ThreadPoolExecutor.CallerRunsPolicy());
		
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			
			internExecutorService.shutdown();
		}));
	};

	public static Path getDatabaseDataDirectoryPath(Path databasePath) {
		return databasePath.resolve("data");
	}

	public static Path getDatabaseIndexDirectoryPath(Path databasePath) {
		return databasePath.resolve("index");
	}

	public static Path getDatabaseDataIndexDirectoryPath(Path databasePath) {
		return getDatabaseIndexDirectoryPath(databasePath).resolve("data");
	}

	public static Path getDatabaseCommitDirectoryPath(Path databasePath) {
		return databasePath.resolve("commit");
	}

	public static Path getDatabaseTxDirectoryPath(Path databasePath) {
		return databasePath.resolve("tx");
	}

	public static Path getDatabaseCommittedPath(Path databasePath) {
		return databasePath.resolve("committed");
	}

	public static Path getDatabaseDeletedPath(Path databasePath) {
		return databasePath.resolve("deleted");
	}

	public static Path getDatabaseIndexPath(Path databasePath, String indexName, long commit) {
		return getDatabaseIndexDirectoryPath(databasePath).resolve(indexName + ".idx." + commit);
	}

	public static Path getDatabaseCommitPath(Path databasePath, long commit) {
		return getDatabaseIndexDirectoryPath(databasePath).resolve("commit." + commit);
	}

	public static OptionalLong getDatabaseCommitPath(Path databasePath) throws IOException {
		return Files.list(getDatabaseIndexDirectoryPath(databasePath)).filter(path -> path.getFileName().toString().matches("commit\\.\\d+"))
				.mapToLong(path -> new VersionFile(path).getVersion()).findFirst();
	}

	public static Path getDatabaseFileIndexPath(Path databasePath, String indexName, long commit) {
		return getDatabaseIndexDirectoryPath(databasePath).resolve(indexName + ".file.idx." + commit);
	}

	public static Path getDatabaseRemoveFilesIndexPath(Path databasePath, long commit) {
		return getDatabaseIndexDirectoryPath(databasePath).resolve("remove.files.idx." + commit);
	}

	public static Path getFileIndexPath(Path databasePath, Path relativeFilePath, String indexName) {
		return DatabaseSupport.getDatabaseDataIndexDirectoryPath(databasePath).resolve(relativeFilePath)
				.resolve(indexName + ".idx");
	}

	public static void executeIntern(Runnable runnable) {
		internExecutorService.submit(runnable);
	}

	public static <K, V> ConcurrentReferenceHashMap<K, V> softValuesMap() {

		return ConcurrentReferenceHashMap.<K, V>builder().strongKeys().softValues().get();
	}

	public static Path temporaryPath(Path path) {

		return path.getParent().resolve(path.getFileName() + ".part");
	}

	public static void deleteDatabaseDirectories(Path databasePath) throws IOException {
	
		Path databaseDataPath = DatabaseSupport.getDatabaseDataDirectoryPath(databasePath);
		DatabaseSupport.removeDirectory(databaseDataPath);

		DatabaseSupport.removeDirectory(databasePath);
	}

	public static void removeDirectory(Path root) throws IOException {

		if(!Files.isDirectory(root))
			return;
		
		Files.walkFileTree(root, new FileVisitor<Path>() {

			@Override
			public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {

				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
				Files.delete(file);
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
				return FileVisitResult.TERMINATE;
			}

			@Override
			public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
				Files.delete(dir);
				return FileVisitResult.CONTINUE;
			}
		});
	}

}
