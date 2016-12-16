package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples in no particular order. Tuples are
 * stored on pages, each of which is a fixed size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

	/**
	 * The File associated with this HeapFile.
	 */
	protected File file;

	/**
	 * The TupleDesc associated with this HeapFile.
	 */
	protected TupleDesc td;

	private Page page;

	/**
	 * Constructs a heap file backed by the specified file.
	 * 
	 * @param f
	 *            the file that stores the on-disk backing store for this heap file.
	 */
	public HeapFile(File f, TupleDesc td) {
		this.file = f;
		this.td = td;
	}

	/**
	 * Returns the File backing this HeapFile on disk.
	 * 
	 * @return the File backing this HeapFile on disk.
	 */
	public File getFile() {
		return file;
	}

	/**
	 * Returns an ID uniquely identifying this HeapFile. Implementation note: you will need to generate this tableid
	 * somewhere ensure that each HeapFile has a "unique id," and that you always return the same value for a particular
	 * HeapFile. We suggest hashing the absolute file name of the file underlying the heapfile, i.e.
	 * f.getAbsoluteFile().hashCode().
	 * 
	 * @return an ID uniquely identifying this HeapFile.
	 */
	public int getId() {
		return file.getAbsoluteFile().hashCode();
	}

	/**
	 * Returns the TupleDesc of the table stored in this DbFile.
	 * 
	 * @return TupleDesc of this DbFile.
	 */
	public TupleDesc getTupleDesc() {
		return td;
	}

	// see DbFile.java for javadocs
	public Page readPage(PageId pid) {
		// some code goes here
		if(getId() == pid.getTableId()) 
        {
            RandomAccessFile randomAccessFile = null;   
            byte[] page = new byte[BufferPool.PAGE_SIZE];   
            try {
                randomAccessFile = new RandomAccessFile(file, "rw");    
                randomAccessFile.seek(pid.pageno());    
                randomAccessFile.readFully(page);;      
            } catch (FileNotFoundException e) {e.printStackTrace(); 
            } catch (IOException e) {e.printStackTrace();}  
            HeapPage heapPage = null;   
            try {
                heapPage = new HeapPage((HeapPageId) pid, page);    
            } catch (IOException e) {e.printStackTrace();}
            try {
                randomAccessFile.close();   
            } catch (IOException e) {e.printStackTrace();}
            return heapPage;    
        }
		throw new UnsupportedOperationException("Implement this");
	}

	// see DbFile.java for javadocs
	public void writePage(Page page) throws IOException {
		// some code goes here
		// not necessary for assignment1
	}

	/**
	 * Returns the number of pages in this HeapFile.
	 */
	public int numPages() {
		return (int) (file.length() / BufferPool.PAGE_SIZE);
	}

	// see DbFile.java for javadocs
	public ArrayList<Page> addTuple(TransactionId tid, Tuple t) throws DbException, IOException,
			TransactionAbortedException {
		// some code goes here
		// not necessary for assignment1
		return null;
	}

	// see DbFile.java for javadocs
	public Page deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
		// some code goes here
		// not necessary for assignment1
		PageId pid = t.getRecordId().getPageId();
		
		if(pid == null)
			throw new DbException("File doesnt contain tuple therefore cant delete it");
		
		if(getId() != pid.getTableId())
			throw new DbException("File doesnt contain tuple therefore cant delete it");
		
		HeapPage curHP = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
		
		curHP.deleteTuple(t);
		return  curHP;
		
		//return null;
	}

	// see DbFile.java for javadocs
	public DbFileIterator iterator(TransactionId tid) {
		// some code goes here
		ArrayList<HeapPage> heappage = new ArrayList<HeapPage>();  
        Iterator<HeapPage> iteratorhp = heappage.iterator();
        BufferPool bufferpool = new BufferPool(heappage.size());    
        page = null;  
        Permissions perm = new Permissions(0); 
        try {
            page = (HeapPage) bufferpool.getPage(tid, page.getId(), perm);  
        } catch (TransactionAbortedException e) {e.printStackTrace();
        } catch (DbException e) {e.printStackTrace();}  
        while(iteratorhp.hasNext()) return (DbFileIterator) iteratorhp; 
		throw new UnsupportedOperationException("Implement this");
	}

}
