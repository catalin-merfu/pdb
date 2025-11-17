package org.merfu.pdb;

import java.util.function.Function;

class ArrayUtils {

	public static <E, K extends Comparable<K>> int binarySearch(E[] elements, int fromIndex, int toIndex, K key, Function<E, K> converter) {
		
        int low = fromIndex;
        int high = toIndex - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            K midVal = converter.apply(elements[mid]);
            int cmp = midVal.compareTo(key);

            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
            	return mid; // key found
        }
        return -(low + 1);  // key not found.
	}

	public static <E, K extends Comparable<K>> int binarySearchFirst(E[] elements, int fromIndex, int toIndex, K key, Function<E, K> converter) {
		
        int low = fromIndex;
        int high = toIndex - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            K midVal = converter.apply(elements[mid]);
            int cmp = midVal.compareTo(key);

            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else {
            	while(--mid >= 0 && key.equals(converter.apply(elements[mid])));
            	
            	return mid + 1; // first key found
            }
        }
        return -(low + 1);  // key not found.
	}
	
	public static <E, T extends Comparable<T>> int binarySearchLast(E[] elements, int fromIndex, int toIndex, T key, Function<E, T> converter) {
		
        int low = fromIndex;
        int high = toIndex - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            T midVal = converter.apply(elements[mid]);
            int cmp = midVal.compareTo(key);

            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else {
            	while(++mid < elements.length && key.equals(converter.apply(elements[mid])));
            	
            	return mid - 1; // first key found
            }
        }
        return -(low + 1);  // key not found.
	}
}
