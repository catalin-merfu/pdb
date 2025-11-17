package org.pdb.db;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;

class LineReader implements Closeable {

	private Reader reader;
	
	private char[] cb;
	private int start;
	private int end;
	
	public LineReader(Path path) throws IOException {
		
		this.reader = new InputStreamReader(Files.newInputStream(path));
		cb = new char[1024 * 128];
	}
	
	public void readLines(Callback callback) throws IOException {

		long position = 0;
		char c;
		end = reader.read(cb, 0, cb.length);
		if(end != -1) {
			c = cb[0];

			while(c == '\r' || c == '\n') {
				if(++start == end) {
					position += start;
					end = reader.read(cb, 0, cb.length);
					if(end != -1) {
						c = cb[0];
						start = 0;
					}
					else
						return;
				}
				else
					c = cb[start];
			}

			position += start;
		}
		else {
			callback.nextLine(null, 0);
			return;
		}

		int i = start;

		for(;;) {
			
			do {
				
				if(++i < end)
					c = cb[i];
				else {
					i -= load();
					
					if(i == end) {
						
						int len = end - start;
						String data = new String(cb, start, len);
						
						callback = callback.nextLine(data, position);
						callback.nextLine(null, position);
						return;
					}
					else
						c = cb[i];
				}
			}
			while(c != '\r' && c != '\n');
			
			int lineSeparatorIdx = i;
			do {
				if(++i < end) {
					c = cb[i];
				}
				else {
					int offset = load();
					lineSeparatorIdx -= offset;
					i -= offset;
					
					if(i == end) {
						
						String data = new String(cb, start, lineSeparatorIdx - start);
	
						callback = callback.nextLine(data, position);
						callback.nextLine(null, position);
						return;
					}
					else
						c = cb[i];
				}
			}
			while(c == '\n' || c == '\r' );
			
			String data = new String(cb, start, lineSeparatorIdx - start);
			callback = callback.nextLine(data, position);
			position += (i - start);
			
			start = i;
		}
	}
	
	private int load() throws IOException {
		
		int offset;
		if(start != 0 && end == cb.length) {
			end -= start;

			for(int i = 0; i < end; i++)
				cb[i] = cb[start + i];
			
			offset = start;
			start = 0;
		}
		else
			offset = 0;
		
		int count = reader.read(cb, end, cb.length - end);
		if(count > 0) {
			end += count;
		}
		else if(count == 0)
			throw new LineTooLong();
		
		return offset;
	}

	@Override
	public void close() throws IOException {
		try {
			reader.close();
		}
		catch(IOException e) {
		}
	}
	
	public interface Callback {
		public Callback nextLine(String line, long position);
	}
}
