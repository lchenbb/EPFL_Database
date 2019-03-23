package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;

import java.util.ArrayList;

public interface ColumnarOperator {


	/**
	 *
	 */
	public boolean is_late_materialization();
	/**
	 * Return the store to upper operator
	 * @return
	 */
	public ArrayList<ColumnStore> get_store();

	/**
	 * This method invokes the execution of the block-at-a-time operator
	 * 
	 * @return each operator returns the set of result columns
	 */
	public DBColumn[] execute();

}
