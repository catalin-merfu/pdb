package org.merfu.pdb;

import java.util.Comparator;

/**
 * A database index for simple string keys
 */
abstract public class StringIndex extends KeyIndex<String> {

	/**
	 * The key value is a the string representation. 
	 */
	@Override
	public String fromKeyString(String keyString) {
		return keyString;
	}
	
	Comparator<String> getKeyStringComparator() {
		return Comparator.naturalOrder();
	}
}
