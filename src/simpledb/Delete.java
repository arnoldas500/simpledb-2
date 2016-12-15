package simpledb;

import java.io.IOException;

/**
 * The delete operator.  Delete reads tuples from its child operator and
 * removes them from the table they belong to.
 */
public class Delete extends AbstractDbIterator {

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * @param t The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
	
	protected DbIterator child; //iterate through the children
	protected TransactionId t;
	protected int afield;
	protected TupleDesc deleteDesc;  // the tupledesc is now defined in Delete Method operator
	protected boolean wasDeleted; // boolean true or false if was deleted
	//Predicate p; //don't think I need this
	
	
	
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
    	this.child = child;
    	this.t = t;
       	String[] nameArray = new String[] {"StagedforDeletion"};//array of deletions string
    	Type[] typeArray = new Type[] {Type.INT_TYPE}; //type of deletions
    	deleteDesc = new TupleDesc(typeArray, nameArray); 
    	
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
    	TupleDesc td = child.getTupleDesc();
    	//Type typeDes = td.getType(afield); //this line may not be necessary
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.open();
    	//on open initialize boolean to false
    	wasDeleted= false;
    	
    }

    public void close() {
        // some code goes here
    	child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.rewind();
    	
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	
    	int sum = 0; //sum is the integer in the tuple which contains the number of deleted records
    	
    	if (wasDeleted){
    		return null; //case when tuple has been deleted we return nothing
    	}
    	
    	while (child.hasNext())
    	{ 
    		Tuple myTuple = child.next();//get the next child and save 
    
    		//should below be in a try/catch block or will the "throws" be sufficient?
    		Database.getBufferPool().deleteTuple(t, myTuple);    			
    		
    		sum++; // add to sum for each deleted tuple
    	}
    	Tuple set = new Tuple(deleteDesc); //set is a tuple containing the total number of tuples staged for deletion
    	set.setField(0, new IntField(sum));
    	wasDeleted = true;
    	return set;
    }
}
