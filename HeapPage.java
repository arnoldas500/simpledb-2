package simpledb;

import java.util.*;
import java.io.*;

/**
 * A {@code HeapPage} is a collection of {@code Tuple}s. A fixed-size memory space is used for each {@code HeapPage}. A
 * {@code HeapFile} consists of {@code HeaPage}s. The {@code HeapPage} class implements the {@code Page} interface that
 * is used by {@code BufferPool}.
 * 
 * @see HeapFile
 * @see BufferPool
 */
public class HeapPage implements Page {

	private static final DataInputStream DataInputStreamin = null;

	/**
	 * The ID of this {@code HeapPage}.
	 */
	HeapPageId pid;

	/**
	 * The {@code TupleDesc} representing the {@code Tuple}s stored in this {@code HeapPage}.
	 */
	TupleDesc td;

	/**
	 * The current content of this {@code HeapPage}.
	 */
	byte[] data;

	/**
	 * The previous image of this {@code HeapPage}.
	 */
	byte[] oldData;

	/**
	 * Creates a {@code HeapPage} from a byte array storing data read from disk. This byte array contains (1) a 4-byte
	 * integer representing the number of {@code Tuple}s assigned to the {@code HeapPage}, (2) a sequence of integer
	 * values each of which indicates where the corresponding {@code Tuple} is stored in the byte array, (3) a free
	 * space reserved for storing additional {@code Tuple}s, and (4) a sequence of {@code Tuple}s.
	 * 
	 * @param id
	 *            the ID of the {@code HeapPage}.
	 * @param data
	 *            a byte array storing the data to read.
	 * @see Database#getCatalog
	 * @see Catalog#getTupleDesc
	 * @see BufferPool#PAGE_SIZE
	 */
	public HeapPage(HeapPageId id, byte[] data) throws IOException {
		this.pid = id;
		this.td = Database.getCatalog().getTupleDesc(id.getTableId());
		this.data = data;
		setBeforeImage();
	}

	/**
	 * Generates a byte array representing the contents of this {@code HeapPage}. This method is used to serialize this
	 * {@code HeapPage} to disk.
	 * <p>
	 * The invariant here is that it should be possible to pass the byte array generated by this method to the
	 * {@code HeapPage} constructor and have it produce an identical {@code HeapPage}.
	 * 
	 * @see #HeapPage
	 * @return a byte array correspond to the content of this {@code HeapPage}.
	 */
	public byte[] getPageData() {
		return data;
	}
	
	

	/**
	 * @return an iterator over all {@code Tuple}s on this {@code HeapPage} (calling remove on this iterator throws an
	 *         {@code UnsupportedOperationException}) (note that this iterator shouldn't return {@code Tuples} in empty
	 *         slots!)
	 */
	
		
	public Iterator<Tuple> iterator() {
		// some code goes here
		return new HeapPageIterator(this);
		//throw new UnsupportedOperationException("Implement this");
	}
	
	class HeapPageIterator implements Iterator<Tuple> {
		
		int m_currentIdx = 0;
		Tuple m_next = null;
		HeapPage m_heapPage;

		public HeapPageIterator(HeapPage p) {
			m_heapPage = p;
			m_next = p.getTuple(0);
		}
		
		public boolean hasNext() {
			if (m_next != null) {
				return true;
			}
			try {
				while (true) {
					m_next = m_heapPage.getTuple(m_currentIdx++);
					if (m_next != null)
						return true;
				}
			} catch (NoSuchElementException e) {
				return false;
			}
		}
		
		public Tuple next() {
			Tuple next = m_next;

			if (next == null) {
				if (hasNext()) {
					next = m_next;
					m_next = null;
					return next;
				} else {
					throw new NoSuchElementException();
				}
			} else {
				m_next = null;
				return next;
			}
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	/**
	 * @return the ID of this {@code HeapPage}.
	 */
	public HeapPageId getId() {
		return pid;
	}
	
	/**
	 * Deletes the specified {@code Tuple} from this {@code HeapPage}; the {@code Tuple} should be updated to reflect
	 * that it is no longer stored on any page.
	 * 
	 * @throws DbException
	 *             if the {@code Tuple} is not on this {@code HeapPage}, or the {@code Tuple} slot is already empty.
	 * @param t
	 *            the {@code Tuple} to delete
	 */
	public void deleteTuple(Tuple t) throws DbException {
		// some code goes here
		
		// not necessary for assignment1
		throw new UnsupportedOperationException("Implement this");
	}

	/**
	 * Adds the specified {@code Tuple} to this {@code HeapPage}; the {@code Tuple} should be updated to reflect that it
	 * is now stored on this {@code HeapPage}.
	 * 
	 * @throws DbException
	 *             if this {@code HeapPage} is full (no empty slots).
	 * @param t
	 *            the {@code Tuple} to add.
	 */
	public void addTuple(Tuple t) throws DbException {
		// some code goes here
		// not necessary for assignment1
		throw new UnsupportedOperationException("Implement this");
	}

	/**
	 * Marks this {@code HeapPage} as dirty/not dirty and record that transaction that did the dirtying
	 */
	public void markDirty(boolean dirty, TransactionId tid) {
		// some code goes here
		// not necessary for assignment1
		throw new UnsupportedOperationException("Implement this");
	}

	/**
	 * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
	 */
	public TransactionId isDirty() {
		// some code goes here
		// Not necessary for assignment1
		throw new UnsupportedOperationException("Implement this");
	}

	/**
	 * Returns {@code Tuple} at the specified entry.
	 * 
	 * @param entryID
	 *            the ID of the entry at which the {@code Tuple} is stored.
	 */
	public Tuple getTuple(int entryID) {
		// some code goes here
		DataInputStream in = new DataInputStream(new ByteArrayInputStream(data, tupleLocation (entryID), data.length
				- tupleLocation(entryID)));
		Tuple T = createTuple(in);
		T.setRecordId(new RecordId (pid, entryID));
		return T;
		//throw new UnsupportedOperationException("Implement this");
	}

	/**
	 * Returns a view of this {@code HeapPage} before it was modified -- used by recovery
	 */
	public HeapPage getBeforeImage() {
		try {
			return new HeapPage(pid, oldData);
		} catch (IOException e) {
			e.printStackTrace();
			// should never happen -- we parsed it OK before!
			System.exit(1);
		}
		return null;
	}

	public void setBeforeImage() {
		oldData = getPageData().clone();
	}

	/**
	 * Generates a byte array corresponding to an empty {@code HeapPage}. This method is used to add new, empty pages to
	 * the file. Passing the results of this method to the {@code HeapPage} constructor will create a {@code HeapPage}
	 * with no valid {@code Tuple}s in it.
	 * 
	 * @return the created byte array.
	 */
	public static byte[] createEmptyPageData() {
		int len = BufferPool.PAGE_SIZE;
		return new byte[len]; // all 0
	}

	/**
	 * Writes an integer value at the specified location of a byte array.
	 * 
	 * @param data
	 *            a byte array.
	 * @param location
	 *            a location in the byte array.
	 * @param value
	 *            the value to write.
	 */
	protected void writeInt(byte[] data, int location, int value) {
		data[location] = (byte) (value >>> 24);
		data[location + 1] = (byte) (value >>> 16);
		data[location + 2] = (byte) (value >>> 8);
		data[location + 3] = (byte) value;
	}

	/**
	 * Reads an integer at the specified location of a byte array.
	 * 
	 * @param data
	 *            a byte array.
	 * @param location
	 *            a location in the byte array.
	 * @return an integer read at the specified location of a byte array.
	 */
	protected int readInt(byte[] data, int location) {
		return ((data[location]) << 24) + ((data[location + 1] & 0xFF) << 16) + ((data[location + 2] & 0xFF) << 8)
				+ (data[location + 3] & 0xFF);
	}

	/**
	 * Returns a byte array representing the specified tuple.
	 * 
	 * @param t
	 *            a tuple.
	 * @return a byte array representing the specified tuple.
	 */
	protected byte[] toByteArray(Tuple t) {
		ByteArrayOutputStream b = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(b);
		try {
			for (int j = 0; j < t.fields.length; j++) {
				Field f = t.getField(j);
				f.serialize(out);
			}
			out.flush();
			b.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return b.toByteArray();
	}

	/**
	 * Constructs a tuple by reading data from the specified {@code DataInputStream}.
	 * 
	 * @param in
	 *            a {@code DataInputStream}.
	 * @return a tuple constructed from the specified {@code DataInputStream}.
	 */
	protected Tuple createTuple(DataInputStream in) {
		try {
			Tuple t = new Tuple(td);
			for (int j = 0; j < td.numFields(); j++) {
				Field f = td.getType(j).parse(in);
				t.setField(j, f);
			}
			return t;
		} catch (java.text.ParseException e) {
			e.printStackTrace();
			throw new NoSuchElementException("parsing error!");
		}
	}

	/**
	 * Returns the end location of the free space in this {@code HeapPage}.
	 * 
	 * @return the end location of the free space in this {@code HeapPage}.
	 */
	protected int endOfFreeSpace() {
		// some code goes here
		// not necessary for assignment1
		throw new UnsupportedOperationException("Implement this");
	}

	/**
	 * Returns the number of entries in this {@code HeapPage}.
	 * 
	 * @return the number of entries in this {@code HeapPage}.
	 */
	protected int entryCount() {
		// some code goes here
		int count = readInt(data,0); 
		return count;
		//throw new UnsupportedOperationException("Implement this");
	}

	/**
	 * Saves (in the header of this {@code HeapPage}) the number of entries managed by this {@code HeapPage}.
	 * 
	 * @param count
	 *            the number of entries in this {@code HeapPage}.
	 */
	protected void saveEntryCount(int count) {
		// some code goes here
		// not necessary for assignment1
		throw new UnsupportedOperationException("Implement this");
	}

	/**
	 * Finds (by examining the header of this {@code HeapPage}) the location of the tuple at the specified entry.
	 * 
	 * @param entryID
	 *            the ID of the entry.
	 * @return the location of the tuple at the specified entry.
	 */
	protected int tupleLocation(int entryID) {
		// some code goes here
		int location = readInt(data,4 + 4 * entryID);
		return location;
		//throw new UnsupportedOperationException("Implement this");
	}

	/**
	 * Saves (in the header of this {@code HeapPage}) the location of the specified tuple.
	 * 
	 * @param entryID
	 *            the ID of the entry at which the tuple is stored.
	 * @param location
	 *            the location of the tuple.
	 */
	protected void saveTupleLocation(int entryID, int location) {
		// some code goes here
		
		// not necessary for assignment1
		throw new UnsupportedOperationException("Implement this");
	}

	/**
	 * Returns the size of free space in this {@code HeapPage}.
	 * 
	 * @return the size of free space in this {@code HeapPage}.
	 */
	protected int sizeOfFreeSpace() {
		return endOfFreeSpace() - sizeOfHeader();
	}

	/**
	 * Returns the size of the header in this {@code HeapPage}.
	 * 
	 * @return the size of the header in this {@code HeapPage}.
	 */
	protected int sizeOfHeader() {
		return 4 + 4 * entryCount();
	}

	/**
	 * Compacts this {@code HeapPage}.
	 */
	protected void compact() {
		// some code goes here
		// not necessary for assignment1
		throw new UnsupportedOperationException("Implement this");
	}

}
