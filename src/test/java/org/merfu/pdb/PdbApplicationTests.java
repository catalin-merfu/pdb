package org.merfu.pdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.merfu.pdb.Database;
import org.merfu.pdb.DatabaseSupport;
import org.merfu.pdb.DelimitedDataSupport;
import org.merfu.pdb.KeyIndex;
import org.merfu.pdb.LookupFunction;
import org.merfu.pdb.MatchedRecord;
import org.merfu.pdb.Pdb;
import org.merfu.pdb.ResponseItem;
import org.merfu.pdb.Transaction;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

@TestInstance(Lifecycle.PER_CLASS)
class PdbApplicationTests {

	FileSystem jimfs = Jimfs.newFileSystem();
	
	Path pdbPath;
	Pdb pdb;
	Database database;
	
	@BeforeAll
	void init() {
		jimfs = Jimfs.newFileSystem(Configuration.unix());
	}
	
	@AfterAll
	void cleanup() throws IOException {
		jimfs.close();
	}
	
	@Test
	void test() throws Exception {
		createPdb();
		createDatabase();
		
		// test installDatabases()
		createPdb();
		
		addFiles();
		queryData(5, 1);
		queryNoRecordTypeData();

		testResponseFormat();

		updateFiles();
		queryData(6, 0);
		
		queryOrderedData();
		queryReversedData();

		removeFiles();
		queryData(0, 4);

		deleteDatabase();
	}
	
	void createPdb() throws IOException {
		pdbPath = jimfs.getPath("/pdb");
		

		KeyIndex<?>[] keyIndexes = new KeyIndex<?>[] {
			new GroupIdIndex(),
			new AddressIndex(),
			new ProvinceCodeIndex()
		};

		pdb = new Pdb(pdbPath, keyIndexes);
		
		assertTrue(Files.isDirectory(pdbPath));
	}
	
	void createDatabase() throws IOException {
		pdb.createDatabase("main");
		
		try(Database database = pdb.getDatabase("main")) {
			assertNotEquals(null, database);
		}
		
		int count = pdb.getInstalledDatabases().size();
		assertEquals(1, count);
	}
	
	void addFiles() throws Exception {
		
		try(Database database = pdb.getDatabase("main")) {
			Transaction transaction = database.beginTransaction();
			
			try {
				Stream.of("BEAVER COUNTY", "GRIM").forEach(name -> {
					try {
						Path resourcePath = Paths.get(ClassLoader.getSystemResource("data/init/" + name).toURI());
						transaction.copyFile(resourcePath, jimfs.getPath("address", name), false);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				});
				
				transaction.copyInputStream(ClassLoader.getSystemResource("data/init/province").openStream(), jimfs.getPath("province"));

				transaction.commit();
				
				Path dataPath = DatabaseSupport.getDatabaseDataDirectoryPath(database.getDatabasePath());
				Stream.of("BEAVER COUNTY", "GRIM").forEach(name -> {
					try {
						assertTrue(Files.isRegularFile(dataPath.resolve("address").resolve(name + ".1")));
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				});
				
				assertTrue(Files.isRegularFile(dataPath.resolve("province.1")));
			}
			catch(Exception ex) {
				transaction.rollback();
				throw ex;
			}
		}
	}
	
	void queryData(long expectedFoundCount, long expectedNotFoundCount) throws IOException {
		
		LookupFunction<String> lookupFunction = LookupFunction.lookupKeyFunction("groupId", this::groupId, 
				LookupFunction.lookupKeyFunction("address", this::address, null));
		
		try(Database database = pdb.getDatabase("main")) {
			try(Reader requestReader = new FileReader(ClassLoader.getSystemResource("request/data").getFile())) {
				Stream<String> requests = new BufferedReader(requestReader).lines();
				List<ResponseItem<String>> response = lookupFunction.lookup(database, requests).toList();
				long foundCount = response.stream().filter(item -> item.getRecord() != null && item.getDataFilePath() != null).count();
				assertEquals(expectedFoundCount, foundCount);
				
				long notFoundCount = response.stream().filter(item -> item.getRecord() == null || item.getDataFilePath() == null).count();
				assertEquals(expectedNotFoundCount, notFoundCount);
			}
		}
	}

	void queryNoRecordTypeData() throws IOException {

		try(Database database = pdb.getDatabase("main")) {
			Stream<MatchedRecord<String>> result = database.lookup("provinceCode", Arrays.asList("ON", "QC", "AB"));
			assertEquals(3, result.filter(rec -> rec.getDataFilePath() != null).count());
		}
	}
	
	void queryOrderedData() throws IOException {
		try(Database database = pdb.getDatabase("main")) {
			
			database.streamOrdered("groupId").reduce((r1, r2) -> {
				
				assertTrue(r1.getKey().compareTo(r2.getKey()) <= 0);
				return r2;
			});
			
			long count = database.streamOrdered("groupId").count();
			assertEquals(34947, count);
			
			database.streamOrdered("groupId", "2545387").reduce((r1, r2) -> {
				
				assertTrue(r1.getKey().compareTo(r2.getKey()) <= 0);
				return r2;
			});
			
			count = database.streamOrdered("groupId", "2551725").count();
			assertEquals(34944, count);
			
			count = database.streamOrdered("groupId", "926340").count();
			assertEquals(1, count);
			
			count = database.streamOrdered("groupId", "926349").count();
			assertEquals(0, count);

			count = database.streamOrdered("groupId", "999999").count();
			assertEquals(0, count);
		}
	}

	void queryReversedData() throws IOException {
		try(Database database = pdb.getDatabase("main")) {
			
			database.streamReversed("groupId").reduce((r1, r2) -> {
				assertTrue(r1.getKey().compareTo(r2.getKey()) >= 0);
				return r2;
			});
			
			long count = database.streamReversed("groupId").count();
			assertEquals(34947, count);
			
			database.streamReversed("groupId", "2545387").reduce((r1, r2) -> {
				
				assertTrue(r1.getKey().compareTo(r2.getKey()) >= 0);
				return r2;
			});
			
			count = database.streamReversed("groupId", "2551728").count();
			assertEquals(5, count);
			
			count = database.streamReversed("groupId", "2545379").count();
			assertEquals(1, count);
			
			count = database.streamReversed("groupId", "2545378").count();
			assertEquals(0, count);

			count = database.streamReversed("groupId", "1111111").count();
			assertEquals(0, count);
		}
	}

	void testResponseFormat() throws IOException, URISyntaxException {
		String response;
		try(Database database = pdb.getDatabase("main")) {
			response = database.<String>streamOrdered("groupId").map(MatchedRecord<String>::getRecord).collect(Collectors.joining());
		}
		
		Path responsePath = new File(ClassLoader.getSystemResource("response/records").toURI()).toPath();
		String expectedResponse = Files.readString(responsePath);
		
		assertEquals(expectedResponse, response);
	}

	void updateFiles() throws IOException {
		try(Database database = pdb.getDatabase("main")) {
			Transaction transaction = database.beginTransaction();

			try {
				try {
					Path updatePath = Paths.get(ClassLoader.getSystemResource("data/update").toURI());
					transaction.copyDir(updatePath, null, false);
			
					Path addPath = Paths.get(ClassLoader.getSystemResource("data/add").toURI());
					transaction.copyDir(addPath, jimfs.getPath("address"), false);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}

				transaction.commit();

				Path dataPath = DatabaseSupport.getDatabaseDataDirectoryPath(database.getDatabasePath());
				Stream.of("GRIM", "RUISSEAU-FERGUSON", "FREDERICTON").forEach(name -> {
					try {
						assertTrue(Files.isRegularFile(dataPath.resolve("address").resolve(name + ".2")));
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				});
			}
			catch(Exception ex) {
				transaction.rollback();
				throw ex;
			}
		}
	}

	void removeFiles() throws IOException {
		try(Database database = pdb.getDatabase("main")) {
			Transaction transaction = database.beginTransaction();

			try {
				try {
					Path addPath = Paths.get(ClassLoader.getSystemResource("data/remove").toURI());
					transaction.copyDir(addPath, jimfs.getPath("address"), false);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}

				transaction.commit();

				Path dataPath = DatabaseSupport.getDatabaseDataDirectoryPath(database.getDatabasePath());
				Stream.of("GRIM", "BEAVER COUNTY", "RUISSEAU-FERGUSON", "FREDERICTON").forEach(name -> {
					try {
						assertTrue(Files.isRegularFile(dataPath.resolve("address").resolve(name + ".3")));
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				});
			}
			catch(Exception ex) {
				transaction.rollback();
				throw ex;
			}
		}
	}

	void deleteDatabase() throws IOException {
		pdb.deleteDatabase("main");
		
		assertFalse(Files.isDirectory(pdbPath.resolve("main")));
		
		assertTrue(pdb.getDatabase("main") == null);
	}
	
	String groupId(String request) {
		return DelimitedDataSupport.field(request, '|', 0);
	}
	
	Address address(String request) {
		String[] fields = request.split("\\|", -1);
		// groupId, streetNo, streetName, streetType, streetDir, unit, postalCode, city, province
		return new Address(fields[8], fields[7], fields[2], fields[3], fields[4], fields[1], fields[5]);
	}
}
