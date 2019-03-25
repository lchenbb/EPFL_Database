package ch.epfl.dias.ops.columnar;

import java.security.cert.CollectionCertStoreParameters;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
	private ArrayList<ColumnStore> stores;
	private boolean is_late;

	public Select(ColumnarOperator child, BinaryOp op, int fieldNo, int value) {
		// TODO: Implement

		this.child = child;
		this.op = op;
		this.fieldNo = fieldNo;
		this.value = value;
		this.is_late = this.child.is_late_materialization();
	}

	@Override
	public boolean is_late_materialization() {

		return this.is_late;
	}
	@Override
	public ArrayList<ColumnStore> get_store() {

		// Return the pointer to self's store
		return this.stores;
	}

	@Override
	public DBColumn[] execute() {
		// TODO: Implement

		// Ask child to load columns
		DBColumn[] columns = this.child.execute();


		// Get updated child's store in case of project
		this.stores = this.child.get_store();

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

			// Late materialization

			// Find store and col to be filtered at

			// Initialize store counter
			int store_index = 0;

			// Initialize col count
			int col_count = 0;

			// Initialize col index to be filter at
			int filter_col_index = 0;

			// Initialize end of finding flag
			boolean localization_end = false;

			// Declare col used container
			List<Integer> col_used;

 			while (!localization_end) {

				// Load current store's col_used
				col_used = this.stores.get(store_index).cols_used;

				// Check whether field to check is in this store
				for (int col_index : col_used) {

					// Forward col_count in this store if not this col to be filtered
					if (!(col_count == this.fieldNo))
						col_count += 1;

					// Break if find col to be filtered
					else {

						// Mark this col as col to be filtered
						filter_col_index = col_index;

						// Set localization_end flag to true
						localization_end = true;

						break;
					}
				}

				// Break if localization ends
				if (localization_end)
					break;

				// Forward store_index else
				store_index += 1;
			}

			// Get current indices of store where col to be filtered resides in
			current_indices = columns[store_index].getAsInteger();

			// Get values to be filtered
			Integer[] target_column = this.stores.get(store_index).getColumns(new int[]{filter_col_index})[0]
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

				case NE:

					if (filter_values[i] != this.value)
						filtered_results[i] = true;
					else
						filtered_results[i] = false;
					break;

			}
		}

		// Return filtered index if late materialization
		if (this.is_late) {

			// Initialize return DBColumn array
			DBColumn[] return_cols = new DBColumn[columns.length];

			// Declare current store's index
			Integer[] current_store_index;

			// Declare filtered index container
			ArrayList<Integer> filtered_index;

			// Get filtered index for each store
			for (int i = 0; i < columns.length; i += 1) {

				// Build filtered index from filtered_result and current index
				filtered_index = new ArrayList<>();

				// Get current store's index
				current_store_index = columns[i].getAsInteger();

				// Get filtered current store's index
				for (int j = 0; j < filtered_results.length; j += 1) {

					if (filtered_results[j])
						filtered_index.add(current_store_index[j]);
				}

				// Push filtered index of current store into return cols
				return_cols[i] = new DBColumn(filtered_index.toArray(),
												DataType.INT);
			}

			// Return filtered col index
			return return_cols;
		}

		// Check whether nothing selected
		boolean null_to_return = true;
		for (int i = 0; i < filtered_results.length; i += 1) {

			if (filtered_results[i]){
				null_to_return = false;
				break;
			}
		}
		if (null_to_return)
			return null;

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
