package ch.epfl.dias.ops.vector;

import java.io.IOException;
import java.util.Arrays;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.ops.volcano.HashJoin;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;
import static org.junit.Assert.*;

import ch.epfl.dias.store.row.DBTuple;
import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.Timeout;

public class VectorTest {

	DataType[] orderSchema;
	DataType[] lineitemSchema;
	DataType[] schema;

	ColumnStore columnstoreData;
	ColumnStore columnstoreOrder;
	ColumnStore columnstoreLineItem;
	ColumnStore columnstoreEmpty;

	Double[] orderCol3 = { 55314.82, 66219.63, 270741.97, 41714.38, 122444.33, 50883.96, 287534.80, 129634.85,
			126998.88, 186600.18 };
	int numTuplesData = 11;
	int numTuplesOrder = 10;
	int standardVectorsize = 3;

	// 1 seconds max per method tested
	@Rule
	public Timeout globalTimeout = Timeout.seconds(1);

	@Before
	public void init() throws IOException {

		schema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT,
				DataType.INT, DataType.INT, DataType.INT, DataType.INT };

		orderSchema = new DataType[] { DataType.INT, DataType.INT, DataType.STRING, DataType.DOUBLE, DataType.STRING,
				DataType.STRING, DataType.STRING, DataType.INT, DataType.STRING };

		lineitemSchema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.DOUBLE,
				DataType.DOUBLE, DataType.DOUBLE, DataType.DOUBLE, DataType.STRING, DataType.STRING, DataType.STRING,
				DataType.STRING, DataType.STRING, DataType.STRING, DataType.STRING, DataType.STRING };

		columnstoreData = new ColumnStore(schema, "input/data.csv", ",");
		columnstoreData.load();

		columnstoreOrder = new ColumnStore(orderSchema, "input/orders_small.csv", "\\|");
		columnstoreOrder.load();

		columnstoreLineItem = new ColumnStore(lineitemSchema, "input/lineitem_small.csv", "\\|");
		columnstoreLineItem.load();

		columnstoreEmpty = new ColumnStore(schema, "input/empty.csv", ",");
		columnstoreEmpty.load();
	}


	@Test
	public void spTestData() {
		/* SELECT COUNT(*) FROM data WHERE col4 == 6 */
		ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreData, standardVectorsize);
		ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.EQ, 3, 6);
		ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(sel,
				Aggregate.COUNT, DataType.INT, 2);

		agg.open();
		DBColumn[] result = agg.next();

		// This query should return only one result
		int output = result[0].getAsInteger()[0];

		assertTrue(output == 3);
	}


	@Test
	public void spTestOrder() {
		/* SELECT COUNT(*) FROM data WHERE col0 == 6 */
		ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreOrder, standardVectorsize);
		ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.EQ, 0, 6);
		ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(sel,
				Aggregate.COUNT, DataType.INT, 2);

		agg.open();
		DBColumn[] result = agg.next();

		// This query should return only one result
		int output = result[0].getAsInteger()[0];

		assertTrue(output == 1);
	}

	@Test
	public void spTestLineItem() {
		/* SELECT COUNT(*) FROM data WHERE col0 == 3 */
		ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, standardVectorsize);
		ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.EQ, 0, 3);
		ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(sel,
				Aggregate.COUNT, DataType.INT, 2);

		agg.open();
		DBColumn[] result = agg.next();

		// This query should return only one result
		int output = result[0].getAsInteger()[0];

		assertTrue(output == 3);
	}

	@Test
	public void test_select(){

		ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreOrder, standardVectorsize);
		ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.NE, 0, 3);

		sel.open();
		sel.next();
	}

	@Test
	public void test_project() {

		ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, standardVectorsize);
		ch.epfl.dias.ops.vector.Project proj = new ch.epfl.dias.ops.vector.Project(scan, new int[]{0});

		proj.open();
		proj.next();
	}

	@Test
	public void test_join() {

		ch.epfl.dias.ops.vector.Scan scan_left = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, standardVectorsize);
		ch.epfl.dias.ops.vector.Scan scan_right = new ch.epfl.dias.ops.vector.Scan(columnstoreOrder, standardVectorsize);

		ch.epfl.dias.ops.vector.Join join = new ch.epfl.dias.ops.vector.Join(scan_left, scan_right, 0, 0);
		join.open();
		join.next();
	}

	@Test
	public void test_agg() {

		ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, standardVectorsize);

		ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(scan, Aggregate.COUNT,
																									DataType.INT, 0);

		agg.open();
		agg.next();
	}

	@Test
	public void joinTest2(){
		/* SELECT COUNT(*) FROM lineitem JOIN order ON (o_orderkey = orderkey) WHERE orderkey = 3;*/

		ch.epfl.dias.ops.vector.Scan scanOrder = new ch.epfl.dias.ops.vector.Scan(columnstoreOrder, standardVectorsize);
		ch.epfl.dias.ops.vector.Scan scanLineitem = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, standardVectorsize);

		/*Filtering on both sides */
		ch.epfl.dias.ops.vector.Select selOrder = new ch.epfl.dias.ops.vector.Select(scanOrder, BinaryOp.EQ,0,3);
		ch.epfl.dias.ops.vector.Select selLineitem = new ch.epfl.dias.ops.vector.Select(scanLineitem, BinaryOp.EQ,0,3);

		Join join = new Join(selLineitem,selOrder,0,0);
		ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(join,Aggregate.COUNT, DataType.INT,0);

		agg.open();
		//This query should return only one result
		DBColumn[] result = agg.next();

		int output = result[0].getAsInteger()[0];
		assertTrue(output == 3);
	}
}
