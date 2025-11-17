package org.pdb.db;

class ArrayNode<K> {

	static int SIZE = 1024;
	
	Object[] elements;
	ArrayNode<K> next;

	ArrayNode() {
		elements = new Object[SIZE];
	}
	
	void set(K data, int index) {

		elements[index] = data;
	}
	
	@SuppressWarnings("unchecked")
	K get(int index) {

		return (K)elements[index];
	}
	
	<T> void copy(T[] destination, int destinationStart) {
		System.arraycopy(elements, 0, destination, destinationStart, SIZE);
	}
	<T> void copy(T[] destination, int destinationStart, int size) {
		System.arraycopy(elements, 0, destination, destinationStart, size);
	}
}
