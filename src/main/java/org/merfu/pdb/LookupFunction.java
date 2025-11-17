package org.merfu.pdb;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterators;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.collections4.IteratorUtils;

/**
 * A function that queries the database based on client provided reference data 
 * 
 * @param <R> The type of the client reference data
 */
public interface LookupFunction<R> {

	/**
	 * Queries the database based on the client provided reference data
	 * 
	 * @param database The database searched
	 * @param requestItems The client request data
	 * @return The response items
	 * 
	 * @throws IOException if an I/O error occurs when reading the file system
	 */
	public Stream<ResponseItem<R>> lookup(Database database, Stream<R> requestItems) throws IOException;

	/**
	 * Creates a lookup function that queries a database index based on keys extracted from the client input and falls back to another provide lookup function if the key were not found.
	 * 
	 * @param <R> The client request data
	 * @param <K> The key type
	 * @param indexName The index name queried
	 * @param referenceDataToKey The function that converts the client request data to a key
	 * @param fallback A fallback lookup function
	 * @return A lookup function
	 */
	public static <R, K extends Comparable<K>> LookupFunction<R> lookupKeyFunction(String indexName, Function<R,K> referenceDataToKey, LookupFunction<R> fallback) {
		return (Database database, Stream<R> requestItems) -> {

				return lookupKeys(database, indexName, requestItems, referenceDataToKey, fallback);
		};
	}

	private static <R, K extends Comparable<K>> Stream<ResponseItem<R>> lookupKeys(Database database, String indexName, Stream<R> requestItems, Function<R,K> referenceDataToKey,
			LookupFunction<R> fallbackLookup) throws IOException {
		
		try {
			List<R> noKeyReferenceDataList = new ArrayLinkedList<>();
			Map<K, List<R>> keyToReferenceData = requestItems.map(request -> new SimpleImmutableEntry<>(referenceDataToKey.apply(request), request)
					).filter(entry -> {
						if(entry.getKey() == null) {
							noKeyReferenceDataList.add(entry.getValue());
							return false;
						}
						return true;
					}).collect(Collectors.groupingBy(SimpleImmutableEntry::getKey, Collectors.mapping(SimpleImmutableEntry::getValue, SinglyLinkedList.collector())));
			Map<K, List<R>> unmatchedkeyToReferenceData = keyToReferenceData.entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
			
			List<K> keys = keyToReferenceData.keySet().stream().collect(ArrayLinkedList.collector());
			Stream<ResponseItem<R>> matchedRecords = database.lookup(indexName, keys).flatMap(p -> {
				K key = p.getKey();
				if(p.getDataFilePath() == null)
					return null;
	
				List<R> referenceDataList = keyToReferenceData.get(key);
				unmatchedkeyToReferenceData.remove(key);
	
				return referenceDataList.stream().map(referenceData -> new ResponseItem<R>(referenceData, p.getRecord(), p.getDataFilePath()));
			});
	
			Stream<R> fallbackUnmatchedReferenceDataStream = StreamSupport.stream(() -> {
					Iterator<R> referenceDataIterator = IteratorUtils.chainedIterator(unmatchedkeyToReferenceData.values().stream().map(Collection::iterator).collect(ArrayLinkedList.collector()));
					return Spliterators.spliteratorUnknownSize(referenceDataIterator, 0);
				}, 0, false);
	
			Stream<ResponseItem<R>> noKeyResponseStream;
			Stream<ResponseItem<R>> fallbackResponseStream;
			
			if(fallbackLookup == null) {
				noKeyResponseStream = noKeyReferenceDataList.stream().map(LookupFunction::toUnmatchedResponse);
				fallbackResponseStream = Stream.of((Void)null).flatMap(x -> fallbackUnmatchedReferenceDataStream.map(LookupFunction::toUnmatchedResponse));
			}
			else {
				noKeyResponseStream = fallbackLookup.lookup(database, noKeyReferenceDataList.stream());
				fallbackResponseStream = Stream.of((Stream<Void>)null).flatMap(x -> {
					try {
						return fallbackLookup.lookup(database, fallbackUnmatchedReferenceDataStream);
					} catch (IOException e) {
						String message = "Failed to lookup in fallback";
						throw new UncheckedIOException(new IOException(message, e));
					}
				});
			}

			return Stream.concat(Stream.concat(noKeyResponseStream.parallel(), matchedRecords).sequential(), fallbackResponseStream);
		}
		catch(UncheckedIOException ex) {
			throw ex.getCause();
		}
	}

	private static <R> ResponseItem<R> toUnmatchedResponse(R unmatchedReferenceData) {
		return new ResponseItem<R>(unmatchedReferenceData, null, null);
	}
}
