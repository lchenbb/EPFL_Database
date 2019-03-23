package ch.epfl.dias.ops.columnar;

import java.security.cert.CollectionCertStoreParameters;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;

public class Select implements ColumnarOperator {

	// TODO: Add required structures

	private ColumnarOperator child;
	private BinaryOp op;
	private int fieldNo;
	private int value;
	private ArrayList<ColumnStore> store;
	private boolean is_late;

	public Select(ColumnarOperator child, BinaryOp op, int fieldNo, int value) {
		// TODO: Implement

		this.child = child;
		this.op = op;
		this.fieldNo = fieldNo;
		this.value = value;
		this.store = this.child.get_store();
		this.is_late = this.child.is_late_materialization();
		this.store = this.child.get_store();
	}

	@Override
	public boolean is_late_materialization() {

		return this.is_late;
	}
	@Override
	public ArrayList<ColumnStore> get_store() {

		// Return the pointer to self's store
		return this.store;
	}

	@Override
	public DBColumn[] execute() {
		// TODO: Implement

		// Ask child to load columns
		DBColumn[] columns = this.child.execute();

		// Return null if null is returned
		if (columns == null)
			return null;

		// Filter the entries satisfying the properties

		// Declare values to be filtered
		Integer[] filter_values;

		// Declare boolean of filtered results
		Boolean[] filtered_results;

		// Dummy initialize current ids for late materialization
		Integer[] current_indices = new Integer[]{0};

		if (!this.is_late) {

			// No late materialization, get data to be filter directly
			filter_values = columns[this.fieldNo].getAsInteger();
			filtered_results = new Boolean[filter_values.length];
		} else {

			// Late materialization, get data to be filter from ids

			// Get current indices
			current_indices = columns[0].getAsInteger();

			// Get values to be filters
			Integer[] target_column = this.store.get(0).getColumns(new int[]{this.fieldNo})[0]
														.getAsInteger();
			filter_values = Arrays.stream(current_indices)
									.map(i -> target_column[i])
									.mapToInt(i -> i)
									.boxed()
									.toArray(Integer[]::new);

			// Initialize boolean filter results
			filtered_results = new Boolean[filter_values.length];
		}

		// Map indices to boolean by checking whether the entry satisfy
		// condition
		for (int i = 0; i < filter_values.length; i += 1){
			// Check correspoinding condition
			switch (this.op){

				case EQ:

					if (filter_values[i] == this.value)
						filtered_results[i] = true;
					else
						filtered_results[i] = false;
					break;

				case LT:

					if (filter_values[i] < this.value)
						filtered_results[i] = true;
					else
						filtered_results[i] = false;
					break;

				case LE:

					if (filter_values[i] <= this.value)
						filtered_results[i] = true;
					else
						filtered_results[i] = false;
					break;

				case GT:

					if (filter_values[i] > this.value)
						filtered_results[i] = true;
					else
						filtered_results[i] = false;
					break;

				case GE:

					if (filter_values[i] >= this.value)
						filtered_results[i] = true;
					else
						filtered_results[i] = false;
					break;

			}
		}

		// Return filtered index if late materialization
		if (this.is_late) {

			// Build filtered index from filtered_result and current index
			ArrayList<Integer> filtered_index = new ArrayList<>();

			for (int i = 0; i < filtered_results.length; i += 1) {

				if (filtered_results[i])
					filtered_index.add(current_indices[i]);
			}

			// Return filtered index
			return new DBColumn[]{new DBColumn(filtered_index.toArray(),
												DataType.INT)};
		}

		// Create filtered columns using map result
		DBColumn[] filtered_cols = new DBColumn[columns.length];

		// Loop to create filtered col
		ArrayList<Object> this_col = new ArrayList<>();
		DBColumn col;
		for (int j = 0; j < filtered_cols.length; j += 1){

			// Get current col
			col = columns[j];

			// Don't know what is the use of eof
			// But let's handle it first
			if (col.eof)
				continue;

			// Collect filtered entries for this col
			for (int i = 0; i < col.entries.length; i += 1){

				if (filtered_results[i])
					this_col.add(col.entries[i]);

			}

			// Add this col to filtered cols
			filtered_cols[j] = new DBColumn(this_col.toArray(), col.type);

			// Empty this_col
			this_col = new ArrayList<>();
		}

		// Return filtered cols
		return filtered_cols;
	}
}
