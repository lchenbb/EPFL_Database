package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.IntStream;

public class Project implements ColumnarOperator {

	// TODO: Add required structures

	private ColumnarOperator child;
	private int[] columns;
	private ArrayList<ColumnStore> store;
	private boolean is_late;

	@Override
	public ArrayList<ColumnStore> get_store() {

		return this.store;
	}

	@Override
	public boolean is_late_materialization() {

		return this.is_late;
	}

	public Project(ColumnarOperator child, int[] columns) {
		// TODO: Implement

		// Initialization
		this.child = child;
		this.columns = columns;

		this.is_late = this.child.is_late_materialization();
		this.store = this.child.get_store();
	}

	public DBColumn[] execute() {
		// TODO: Implement

		// Return current indices if late materialization
		if (this.is_late)
			return this.child.execute();

		// Ask child to execute
		DBColumn[] column_values = this.child.execute();

		// Return required cols
		if (column_values == null)
			return null;

		// Initialize projection cols
		ArrayList<DBColumn> proj_cols = new ArrayList();

		// Populate proj_cols
		for (int i : columns)
			proj_cols.add(column_values[i]);

		// Return proj cols
		return proj_cols.toArray(new DBColumn[proj_cols.size()]);

	}
}
