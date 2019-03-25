package ch.epfl.dias.ops.volcano;

import static org.junit.Assert.*;

import java.io.IOException;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;
import ch.epfl.dias.store.row.RowStore;
import ch.epfl.dias.store.PAX.DBPAXpage;
import ch.epfl.dias.store.PAX.PAXStore;

import org.junit.Before;
import org.junit.Test;

public class VolcanoTest {

    DataType[] orderSchema;
    DataType[] lineitemSchema;
    DataType[] schema;
    
    RowStore rowstoreData;
    RowStore rowstoreOrder;
    RowStore rowstoreLineItem;

    PAXStore PAXstoreData;
    PAXStore PAXstoreOrder;
    PAXStore PAXstoreLineItem;

    @Before
    public void init() throws IOException  {
    	
		schema = new DataType[]{ 
				DataType.INT, 
				DataType.INT, 
				DataType.INT, 
				DataType.INT, 
				DataType.INT,
				DataType.INT, 
				DataType.INT, 
				DataType.INT, 
				DataType.INT, 
				DataType.INT };
    	
        orderSchema = new DataType[]{
                DataType.INT,
                DataType.INT,
                DataType.STRING,
                DataType.DOUBLE,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.INT,
                DataType.STRING};

        lineitemSchema = new DataType[]{
                DataType.INT,
                DataType.INT,
                DataType.INT,
                DataType.INT,
                DataType.DOUBLE,
                DataType.DOUBLE,
                DataType.DOUBLE,
                DataType.DOUBLE,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING};
        
        rowstoreData = new RowStore(schema, "input/data.csv", ",");
        rowstoreData.load();
        
        rowstoreOrder = new RowStore(orderSchema, "input/orders_small.csv", "\\|");
        rowstoreOrder.load();
        
        rowstoreLineItem = new RowStore(lineitemSchema, "input/lineitem_small.csv", "\\|");
        rowstoreLineItem.load();


		PAXstoreData = new PAXStore(schema, "input/data.csv", ",", 50);
		PAXstoreData.load();

		PAXstoreOrder = new PAXStore(orderSchema, "input/orders_small.csv", "\\|", 50);
		PAXstoreOrder.load();

		PAXstoreLineItem = new PAXStore(lineitemSchema, "input/lineitem_small.csv", "\\|", 50);
		PAXstoreLineItem.load();

	}
    
	@Test
	public void spTestData(){
	    /* SELECT COUNT(*) FROM data WHERE col4 == 6 */	    
	    ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreData);
	    ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.EQ, 3, 6);
	    ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, 2);
	
		agg.open();
		
		// This query should return only one result
		DBTuple result = agg.next();
		int output = result.getFieldAsInt(0);
		assertTrue(output == 3);
	}
	
	@Test
	public void spTestOrder(){
	    /* SELECT COUNT(*) FROM data WHERE col0 == 6 */	    
	    ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreOrder);
	    ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.EQ, 0, 6);
	    ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, 2);
	
		agg.open();
		
		// This query should return only one result
		DBTuple result = agg.next();
		int output = result.getFieldAsInt(0);
		assertTrue(output == 1);
	}
	
	@Test
	public void spTestLineItem(){
	    /* SELECT COUNT(*) FROM data WHERE col0 == 3 */	    
	    ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
	    ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.EQ, 0, 3);
	    ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, 2);
	
		agg.open();
		
		// This query should return only one result
		DBTuple result = agg.next();
		int output = result.getFieldAsInt(0);
		assertTrue(output == 3);
	}

	@Test
	public void joinTest1(){
	    /* SELECT COUNT(*) FROM order JOIN lineitem ON (o_orderkey = orderkey) WHERE orderkey = 3;*/
	
		ch.epfl.dias.ops.volcano.Scan scanOrder = new ch.epfl.dias.ops.volcano.Scan(rowstoreOrder);
		ch.epfl.dias.ops.volcano.Scan scanLineitem = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
	
	    /*Filtering on both sides */
	    Select selOrder = new Select(scanOrder, BinaryOp.EQ,0,3);
	    Select selLineitem = new Select(scanLineitem, BinaryOp.EQ,0,3);
	
	    HashJoin join = new HashJoin(selOrder,selLineitem,0,0);
	    ProjectAggregate agg = new ProjectAggregate(join,Aggregate.COUNT, DataType.INT,0);
	
	    agg.open();
	    //This query should return only one result
	    DBTuple result = agg.next();
	    int output = result.getFieldAsInt(0);
	    assertTrue(output == 3);
	}
	
	@Test
	public void joinTest2(){
	    /* SELECT COUNT(*) FROM lineitem JOIN order ON (o_orderkey = orderkey) WHERE orderkey = 3;*/
	
		ch.epfl.dias.ops.volcano.Scan scanOrder = new ch.epfl.dias.ops.volcano.Scan(rowstoreOrder);
		ch.epfl.dias.ops.volcano.Scan scanLineitem = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
	
	    /*Filtering on both sides */
	    Select selOrder = new Select(scanOrder, BinaryOp.EQ,0,3);
	    Select selLineitem = new Select(scanLineitem, BinaryOp.EQ,0,3);
	
	    HashJoin join = new HashJoin(selLineitem,selOrder,0,0);
	    ProjectAggregate agg = new ProjectAggregate(join,Aggregate.COUNT, DataType.INT,0);
	
	    agg.open();
	    //This query should return only one result
	    DBTuple result = agg.next();
	    int output = result.getFieldAsInt(0);
	    assertTrue(output == 3);
	}

	@Test
	public void test_select(){

		ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(PAXstoreOrder);
		ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.GE, 0, 3);
		sel.open();
		DBTuple result = sel.next();
		result.getFieldAsInt(0);
	}

	@Test
	public void test_project(){

		ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(PAXstoreOrder);
		ch.epfl.dias.ops.volcano.Project proj = new ch.epfl.dias.ops.volcano.Project(scan, new int[]{0, 1});
		proj.open();
		proj.next();
	}

	@Test
	public void test_join(){

		ch.epfl.dias.ops.volcano.Scan scan_left = new ch.epfl.dias.ops.volcano.Scan(rowstoreOrder);
		ch.epfl.dias.ops.volcano.Scan scan_right = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);

		ch.epfl.dias.ops.volcano.HashJoin join = new ch.epfl.dias.ops.volcano.HashJoin(scan_left, scan_right,0, 0);
		join.open();

		join.next();
	}

	@Test
	public void test_agg(){

    	ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreOrder);
    	ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(scan, Aggregate.COUNT, DataType.INT, 0);

    	agg.open();

    	agg.next();
	}
}
