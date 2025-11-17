package org.pdb.db;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.stream.Collector;

class ArrayLinkedList<K> implements List<K> {

	private ArrayNode<K> head;
	private ArrayNode<K> tail;
	private int position;
	
	public ArrayLinkedList() {
		head = tail = new ArrayNode<>();
	}

	static public <T> Collector<T, ?, List<T>> collector() {
		return Collector.of(ArrayLinkedList<T>::new, (a, e) -> a.add(e), (a1, a2) -> {a1.addAll(a2); return a1;});
	}
	
	@Override
	public int size() {
		ArrayNode<K> node = head;
		int size = 0;
		while(node.next != null) {
			size += ArrayNode.SIZE;
			node = node.next;
		}
		
		size += position;
		return size;
	}

	@Override
	public boolean isEmpty() {
		return position == 0;
	}

	@Override
	public boolean contains(Object o) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterator<K> iterator() {
		return new Iterator<>() {

			ArrayNode<K> node;
			Object[] elements;
			int idx;
			int nodeSize;
			
			{
				if(position > 0)
				{
					node = head;
					elements = head.elements;
					nodeSize = nodeSize(head);
				}
			}
			
			@Override
			public boolean hasNext() {
				return node != null;
			}

			@Override
			public K next() {
				@SuppressWarnings("unchecked")
				K data = (K)elements[idx++];
				if(idx == nodeSize) {
					node = node.next;
					if(node != null) {
						elements = node.elements;
						idx = 0;
						nodeSize = nodeSize(node);
					}
				}
				return data;
			}
			
			private int nodeSize(ArrayNode<K> node) {
				if(node.next == null)
					return position;
				return ArrayNode.SIZE;
			}
		};
	}

	@Override
	public Object[] toArray() {
		return toArray(new Object[size()]);
	}

	@Override
	public <T> T[] toArray(T[] a) {

		ArrayNode<K> node = head;
		int destinationStart = 0;
		while(node.next != null) {
			node.copy(a, destinationStart);
			destinationStart += ArrayNode.SIZE;
			node = node.next;
		}

		node.copy(a, destinationStart, position);

		return a;
	}

	@Override
	public boolean add(K e) {
		if(position < ArrayNode.SIZE)
			tail.set(e, position++);
		else {
			tail.next = new ArrayNode<>();
			tail = tail.next;
			
			tail.set(e, 0);
			position = 1;
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
		throw new UnsupportedOperationException();
	}

	@Override
	public K get(int index) {
		ArrayNode<K> node = head;
		while(index >= ArrayNode.SIZE) {
			node = node.next;
			index -= ArrayNode.SIZE;
		}
		return node.get(index);
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
