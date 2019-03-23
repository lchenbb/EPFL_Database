package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.ops.volcano.VolcanoOperator;
import ch.epfl.dias.store.column.DBColumn;
import com.sun.org.apache.bcel.internal.generic.BIPUSH;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;

public class Select implements VectorOperator {

	// TODO: Add required structures

	private VectorOperator child;
	private BinaryOp op;
	private int fieldNo;
	private int value;

	public Select(VectorOperator child, BinaryOp op, int fieldNo, int value) {
		// TODO: Implement

		// Initialization
		this.child = child;
		this.op = op;
		this.fieldNo = fieldNo;
		this.value = value;

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

		// Declare holder of child's next bunch of columns
		DBColumn[] child_next_cols;

		// Initialize holder for filtered index
		ArrayList<Integer> filtered_index = new ArrayList<>();

		// Declare holder for col to be check
		Integer[] target_col;

		// Pull till some rows pass filter or null returned
		while ((child_next_cols = this.child.next()) != null) {

			// Get target col
			target_col = child_next_cols[this.fieldNo].getAsInteger();

			// Check which rows satisfy predicate
			for (int i = 0; i < target_col.length; i += 1) {

				switch (this.op) {

					case GT:

						if (target_col[i] > this.value)
							filtered_index.add(i);
						break;

					case GE:

						if (target_col[i] >= this.value)
							filtered_index.add(i);
						break;

					case LE:

						if (target_col[i] <= this.value)
							filtered_index.add(i);
						break;

					case LT:

						if (target_col[i] < this.value)
							filtered_index.add(i);
						break;

					case NE:

						if (target_col[i] != this.value)
							filtered_index.add(i);
						break;

					case EQ:

						if (target_col[i] == this.value)
							filtered_index.add(i);
						break;
				}
			}

			// Continue to pull next bunch if filtered index is empty
			if (filtered_index.size() == 0)
				continue;

			// Construct filtered cols
			ArrayList<ArrayList<Object>> filtered_values = new ArrayList<>();
			for (int i = 0; i < child_next_cols.length; i += 1)
				filtered_values.add(new ArrayList<>());

			// Visit selected rows
			for (int i : filtered_index) {

				// Visit every col
				for (int j = 0; j < child_next_cols.length; j += 1)
					filtered_values.get(j).add(child_next_cols[j].entries[i]);
			}

			// Initialize return DBColumn holder
			ArrayList<DBColumn> filtered_cols = new ArrayList<>();

			// Map from col values to col
			for (int k = 0; k < filtered_values.size(); k += 1) {

				filtered_cols.add(new DBColumn(filtered_values.get(k).toArray(),
						child_next_cols[k].type));
			}

			// Return filtered cols
			return filtered_cols.toArray(new DBColumn[filtered_cols.size()]);

		}

		return null;
	}

	@Override
	public void close() {
		// TODO: Implement

		// Ask child to close
		this.child.close();
	}


	/**
	 * Merge two DBColumns with same type
	 * @param a
	 * @param b
	 * @return merged col
	 */
	private DBColumn merge_col(DBColumn a, DBColumn b) {

		// Deal with null case
		if (a == null)
			return b;

		// Initialize container for value in two cols
		ArrayList<Object> value_container = new ArrayList<>();

		// Add value of each col to container
		for (Object x : a.entries)
			value_container.add(x);

		for (Object x : b.entries)
			value_container.add(x);
		// Build and return merged DBColumn
		return new DBColumn(value_container.toArray(), a.type);
	}
}
