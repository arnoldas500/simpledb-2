package simpledb;

import java.util.*;

import simpledb.Aggregator.Op;

/**
 * An {@code Aggregate} operator computes an aggregate value (e.g., sum, avg, max, min) over a single column, grouped by a
 * single column.
 */
public class Aggregate extends AbstractDbIterator {

	/**
	 * Constructs an {@code Aggregate}.
	 *
	 * Implementation hint: depending on the type of afield, you will want to construct an {@code IntAggregator} or
	 * {@code StringAggregator} to help you with your implementation of {@code readNext()}.
	 * 
	 *
	 * @param child
	 *            the {@code DbIterator} that provides {@code Tuple}s.
	 * @param afield
	 *            the column over which we are computing an aggregate.
	 * @param gfield
	 *            the column over which we are grouping the result, or -1 if there is no grouping
	 * @param aop
	 *            the {@code Aggregator} operator to use
	 */
	
	DbIterator child;
	int afield;
	int gfield;
	Aggregator.Op aop;
	Aggregator agg;
	DbIterator aggIterator;
	
	public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
		// some code goes here
		this.child = child;
		this.afield = afield;
		this.gfield = gfield;
		this.aop = aop;
		aggIterator = null;
		
		Type gbType;
    	
    	if (gfield == Aggregator.NO_GROUPING)
    		gbType = null;
    	else 
    		gbType = child.getTupleDesc().getType(gfield);
    	
    	Type aggType = child.getTupleDesc().getType(afield);
    	switch(aggType)
    	{
    		case INT_TYPE: 
    			agg = new IntAggregator(gfield, gbType, afield, aop);
    			break;
    		case STRING_TYPE:
    			agg = new StringAggregator(gfield, gbType, afield, aop);
    			break;
    		default:
    			assert(false);
    	}
		
	}

	public static String aggName(Aggregator.Op aop) {
		switch (aop) {
		case MIN:
			return "min";
		case MAX:
			return "max";
		case AVG:
			return "avg";
		case SUM:
			return "sum";
		case COUNT:
			return "count";
		}
		return "";
	}

	public void open() throws NoSuchElementException, DbException, TransactionAbortedException {
		// some code goes here
		child.open();
    	while (child.hasNext())
    	{
    		agg.merge(child.next());
    	}
    	aggIterator = agg.iterator();
    	aggIterator.open();
		//this.child.open();
	}

	/**
	 * Returns the next {@code Tuple}. If there is a group by field, then the first field is the field by which we are
	 * grouping, and the second field is the result of computing the aggregate, If there is no group by field, then the
	 * result tuple should contain one field representing the result of the aggregate. Should return {@code null} if
	 * there are no more {@code Tuple}s.
	 */
	protected Tuple readNext() throws TransactionAbortedException, DbException {
		// some code goes here
		if(aggIterator.hasNext())
			return aggIterator.next(); 
		else
			return null;
	}

	public void rewind() throws DbException, TransactionAbortedException {
		// some code goes here
		aggIterator.rewind();
	}

	/**
	 * Returns the {@code TupleDesc} of this {@code Aggregate}. If there is no group by field, this will have one field
	 * - the aggregate column. If there is a group by field, the first field will be the group by field, and the second
	 * will be the aggregate value column.
	 * 
	 * The name of an aggregate column should be informative. For example:
	 * {@code aggName(aop) (child_td.getFieldName(afield))} where {@code aop} and {@code afield} are given in the
	 * constructor, and {@code child_td} is the {@code TupleDesc} of the child iterator.
	 */
	public TupleDesc getTupleDesc() {
		// some code goes here
		return child.getTupleDesc();
		//return null;
	}

	public void close() {
		// some code goes here
		child.close();
    	aggIterator.close();
	}
}
