package org.merfu.pdb;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.merfu.pdb.DelimitedDataSupport;

class DelimitedDataSupportTest {

	private static String request = "field0||field2";
	private static char separator = '|';
	
	@Test
	void testField() {
		assertEquals("field0", DelimitedDataSupport.field(request, separator, 0));
		assertEquals("field2", DelimitedDataSupport.field(request, separator, 2));
	
		assertEquals(null, DelimitedDataSupport.field(request, separator, 1));
		assertEquals(null, DelimitedDataSupport.field(request, separator, 3));
	}

	@Test
	void testFields() {
		assertEquals("field0|", DelimitedDataSupport.fields(request, separator, 0, 1));
		assertEquals("|field2", DelimitedDataSupport.fields(request, separator, 1, 2));

		assertEquals(null, DelimitedDataSupport.fields(request, separator, 3, 4));
	}

	@Test
	void testFieldsSplit() {
		assertEquals(3, DelimitedDataSupport.fields("field0|fields1|field2", '|').length);
	}
}
