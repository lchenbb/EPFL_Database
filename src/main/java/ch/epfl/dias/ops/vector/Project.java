package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

public class Project implements VectorOperator {

	// TODO: Add required structures

	private VectorOperator child;
	private int[] fieldNo;

	public Project(VectorOperator child, int[] fieldNo) {
		// TODO: Implement

		// Initialization
		this.child = child;
		this.fieldNo = fieldNo;

	}

	@Override
	public void open() {
		// TODO: Implement

		// Ask child to open
		this.child.open();
	}

	@Override
	public DBColumn[] next() {
		// TODO: Implement

		// Pull next bunch of data from child
		DBColumn[] child_next = this.child.next();

		// Return null if null returned
		if (child_next == null)
			return null;

		// Projection
		DBColumn[] proj_next = new DBColumn[this.fieldNo.length];

		for (int i = 0; i < this.fieldNo.length; i += 1)
			proj_next[i] = child_next[this.fieldNo[i]];

		// Return projection
		return proj_next;
	}

	@Override
	public void close() {
		// TODO: Implement

		// Ask child to close
		this.child.close();
	}
}
