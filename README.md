Pdb is a Java library that indexes data records in text files formatted in lines separated by LF/CR characters.

# Package Dependency

Add the following dependency to your project:

		<dependency>		
			<groupId>org.merfu</groupId>
			<artifactId>pdb</artifactId>
			<version>1.0.0</version>
		</dependency>

# Usage

This documentation walks through an example that indexes a database containing
files of person records that are identified by last and first name and contain address details data.

##The data files and the directory structure
The persons data is contained in files with this format:

	H|...
	P|last_name>|first_name|...|other_indexable_fields|...
	A|city|street|number|...
	...
	T|...

The lines starting with P are the header line records and contain the indexable fields
The A lines are the record details lines and do not contain indexable fields.
Both the header line and the details lines are returned in the client responses.

The files are organized in a directory structure:

	\root
		\subdir1
			\file1
		\subdir2
			\file1
			\file2

## Implement the persons file format
The person records start with a header line and the first field in the record header is the record type 'P':

	public class PersonFileFormat extends StandardFileFormat {
	
		@Override
		public boolean isRecordHeaderLine(String line) {
		
			return line.startsWith("P|");
		}
	}

## Implement the person name index and the persons file indexer
For each index that must be build on the data files implement a KeyIndexer and a KeyIndex. The KeyIndexer extracts
the key strings from the files and provides the FileFormat. The KeyIndex models the index itself and is used by  
Pdb when querying the files and when building the index.

Implement the KeyIndex interface for all key types other that String. For the String key type extend StringIndex.

All the files in this example are of the same format and should be indexed by this KeyIndex:

	public class PersonIndex extends KeyIndex<Person> {
	
		@Override
		public String getName() {
			return "person";
		}
	
		@Override
		public Person fromKeyString(String keyString) {
			String fields[] = keyString.split("\\|");
			return new Person(fields[1], fields[0]);
		}
	
		@Override
		public boolean canIndex(Path relativePath, boolean isDirectory) {
			return true;
		}
	
		@Override
		public KeyIndexer&lt;Person> getKeyIndexer(Path relativePath) {
			
			return new PersonIndexer();
		}
	}

Implement KeyIndexer to define indexers for key types other than String. For the String key type extend StringIndexer:

	public class PersonIndexer implements KeyIndexer<Person> {
	
		@Override
		public String keyStringFromLine(String line) {
	
			String fields[] = line.split("\\|");
	
			return Stream.of(fields[1], fields[2]).collect(Collectors.joining("|"));
		}
	
		@Override
		public FileFormat getFileFormat() {
			return new PersonFileFormat();
		}
	}

## Create and populate the database

Always get the database in try-with-resources:

		Pdb pdb = new Pdb(Path.of("/pdb"), new KeyIndex<>[] {new PersonIndex()});
		
		pdb.createDatabase("main");
		
		Transaction transaction;
		try(Database database = pdb.getDatabase("main")) {
			transaction = database.beginTransaction();
		}

		transaction.copyDir(Path.of("/local/fs/data"), null, false);
		transaction.commit();

## Query the database

		try(Database database = pdb.getDatabase("main")) {
			Stream<MatchedRecord<Person>> persons = database.lookup("person", Arrays.asList(new Person("LastName", "FirstName")));
			persons.forEach(person -> System.out.print(person.getRecord()));
		}
