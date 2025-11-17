package org.pdb.db;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.stream.Collector;

class SinglyLinkedList<K> implements List<K> {

	Node<K> head;
	Node<K> tail;
	
	static public <T> Collector<T, ?, List<T>> collector() {
		return Collector.of(SinglyLinkedList<T>::new, (a, e) -> a.add(e), (a1, a2) -> {a1.addAll(a2); return a1;});
	}
	
	@Override
	public int size() {
		Node<K> node = head;
		int size = 0;
		while(node != null) {
			node = node.next;
			size++;
		}
		return size;
	}

	@Override
	public boolean isEmpty() {
		return tail == null;
	}

	@Override
	public boolean contains(Object o) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterator<K> iterator() {
		
		return new Iterator<K>() {

			Node<K> last = head;
			
			@Override
			public boolean hasNext() {
				return last != null;
			}

			@Override
			public K next() {
				K data = last.data;
				last = last.next;
				
				return data;
			}
			
		};
	}

	@Override
	public Object[] toArray() {
		
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T[] toArray(T[] a) {
		int i = 0;
		Node<K> node = head;
		while(node != null) {
			a[i++] = (T)node.data;
			node = node.next;
		}
		return a;
	}

	@Override
	public boolean add(K e) {
		
		Node<K> node = new Node<>(e);
		if(tail == null)
			head = tail = node;
		else {
			tail.next = node;
			tail = node;
		}
		
		return true;
	}

	@Override
	public boolean remove(Object o) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean addAll(Collection<? extends K> c) {
		c.forEach(e -> add(e));
		
		return true;
	}

	@Override
	public boolean addAll(int index, Collection<? extends K> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void clear() {
		head = tail = null;
	}

	@Override
	public K get(int index) {
		throw new UnsupportedOperationException();
	}

	@Override
	public K set(int index, K element) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void add(int index, K element) {
		throw new UnsupportedOperationException();
	}

	@Override
	public K remove(int index) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int indexOf(Object o) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int lastIndexOf(Object o) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ListIterator<K> listIterator() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ListIterator<K> listIterator(int index) {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<K> subList(int fromIndex, int toIndex) {
		throw new UnsupportedOperationException();
	}
}
