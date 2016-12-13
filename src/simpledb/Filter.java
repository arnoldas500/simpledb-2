package simpledb;  

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends AbstractDbIterator {

	/**
	 * The child operator.
	 */
	DbIterator child;
	
	/**
	 * The predicate.
	 */
	Predicate p;
	
    /**
     * Constructor accepts a predicate to apply and a child
     * operator to read tuples to filter from.
     *
     * @param p The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, DbIterator child) {
    	this.child = child;	
    	this.p = p;
    	
    }

    public TupleDesc getTupleDesc() {
    	return child.getTupleDesc();
    }

    public void open()
        throws DbException, NoSuchElementException, TransactionAbortedException {
    	child.open();
    }

    public void close() {
    	child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	child.rewind();   	
    }

    /**
     * AbstractDbIterator.readNext implementation.
     * Iterates over tuples from the child operator, applying the predicate
     * to them and returning those that pass the predicate (i.e. for which
     * the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no more tuples
     * @see Predicate#filter
     */
    /*
     * (non-Javadoc)
     * @see simpledb.AbstractDbIterator#readNext()
     * finished on 12/12/2016 at 19:18, worked on it for 3 hours today and 3 hours 
     * on saturday
     * When called method loops while the db iterator has next evaluates to true
     * create a tuple and set the instance of the next child element to store it and then 
     * check if the predicate.filter of the child (the one stored in t1) returns true
     * if so then return it and keep looping to return all of them, when done and there is
     * no more tuples then return null
     * worked together on this method with kevin
     */
    protected Tuple readNext()
        throws NoSuchElementException, TransactionAbortedException, DbException {
        while(child.hasNext()){ //while the db iterator isnt run keep looping
        	Tuple t1 = child.next(); // get the next child and store it in tuple t1
        	if(p.filter(t1)){ //if the previous.filter of the child is true then return it
        		System.out.println(t1);
        		return t1; //returns the next tuple 
        	}
        }
        return null; // return null  if there are no more tuples 
    }
}
