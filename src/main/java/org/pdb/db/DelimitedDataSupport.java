package org.pdb.db;

import java.util.LinkedList;
import java.util.List;

/**
 * Utilities to help parsing delimited data
 */
public class DelimitedDataSupport {

	/**
	 * Returns the field for the column index.
	 * 
	 * @param data Delimited data
	 * @param delimiter The fields delimiter in the client data 
	 * @param column The zero-based column index
	 * @return The field value
	 */
	public static String field(String data, char delimiter, int column) {
		
		int startFieldIdx = 0;
		for(; column > 0; column--) {
			startFieldIdx = data.indexOf(delimiter, startFieldIdx) + 1;
			if(startFieldIdx == 0)
				return null;
		}

		int endFieldIdx = data.indexOf(delimiter, startFieldIdx);
		
		String field = endFieldIdx < 0 ? data.substring(startFieldIdx) : data.substring(startFieldIdx, endFieldIdx);
		return field.isBlank() ? null : field;
	}

	/**
	 * Returns the fields between 2 column indexes.
	 * @param data Delimited data
	 * @param delimiter The fields delimiter in the client data 
	 * @param startColumn The zero-based start column index
	 * @param endColumn The zero-based end column index
	 * @return The delimited fields between the start and end columns
	 */
	public static String fields(String data, char delimiter, int startColumn, int endColumn) {
		
		int startFieldIdx = 0;
		for(int column = startColumn; column > 0; column--) {
			startFieldIdx = data.indexOf(delimiter, startFieldIdx) + 1;
			if(startFieldIdx == 0)
				return null;
		}

		int endFieldIdx = startFieldIdx + 1;
		for(int column = endColumn - startColumn; column > 0; column--) {
			endFieldIdx = data.indexOf(delimiter, endFieldIdx);
			if(endFieldIdx == -1)
				break;
			
			endFieldIdx++;
		}

		String field = endFieldIdx == -1 ? data.substring(startFieldIdx) : data.substring(startFieldIdx, endFieldIdx);
		return field;
	}
	
	/**
	 * Splits the request item into a String array using the delimiter provided.
	 * 
	 * @param data Delimited data
	 * @param delimiter The character used for fields demarcation
	 * @return The fields in the input data
	 */
	public static String[] fields(String data, char delimiter) {

		List<String> fields = new LinkedList<String>();
		
		int fieldStartIdx = 0;
		int nextDelIdx = data.indexOf(delimiter, fieldStartIdx);
		while(nextDelIdx >= 0) {
			fields.add(data.substring(fieldStartIdx, nextDelIdx));
			fieldStartIdx = nextDelIdx + 1;
			nextDelIdx = data.indexOf(delimiter, fieldStartIdx);
		}
		
		fields.add(data.substring(fieldStartIdx));
		return fields.toArray(new String[fields.size()]);
	}
}
