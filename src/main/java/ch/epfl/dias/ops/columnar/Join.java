package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class Join implements ColumnarOperator {

	// TODO: Add required structures

	private ColumnarOperator leftChild;
	private ColumnarOperator rightChild;
	private int leftFieldNo;
	private int rightFieldNo;
	private ArrayList<ColumnStore> left_stores;
	private ArrayList<ColumnStore> right_stores;
	private boolean is_late;

	@Override
	public ArrayList<ColumnStore> get_store() {

		// Return the combination of left and right store
		return new ArrayList<>(Stream.concat(this.left_stores.stream(),
												this.right_stores.stream())
										.collect(Collectors.toList()));
	}

	@Override
	public boolean is_late_materialization(){

		return this.is_late;
	}

	public Join(ColumnarOperator leftChild, ColumnarOperator rightChild, int leftFieldNo, int rightFieldNo) {
		// TODO: Implement

		// Initialization
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.leftFieldNo = leftFieldNo;
		this.rightFieldNo = rightFieldNo;

		this.is_late = this.leftChild.is_late_materialization();
	}

	public DBColumn[] execute() {
		// TODO: Implement

		// Ask left child to get data
		DBColumn[] left_cols = this.leftChild.execute();

		// Ask right child to get data
		DBColumn[] right_cols = this.rightChild.execute();

		// Get store of children
		this.left_stores = this.leftChild.get_store();
		this.right_stores = this.rightChild.get_store();

		// Handle late materialization
		if (this.is_late) {

			// Get cols to compare

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

			// Find left store and col used to compare
			while (!localization_end) {

				// Load left store's col_used
				col_used = this.left_stores.get(store_index).cols_used;

				// Check whether field to check is in this store
				for (int col_index : col_used) {

					// Forward col_count in this store if not this col to be filtered
					if (!(col_count == this.leftFieldNo))
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

			// Find left to be compared col's values
			Object[] left_col_values = this.left_stores.get(store_index).columns.get(filter_col_index).entries;

			// Find left to be compared col's row feasible row indices
			Integer[] left_col_row_indices = left_cols[store_index].getAsInteger();


			// Find right store and col used to compare
			// Reset
			store_index = 0;
			col_count = 0;
			filter_col_index = 0;
			localization_end = false;

			while (!localization_end) {

				// Load left store's col_used
				col_used = this.right_stores.get(store_index).cols_used;

				// Check whether field to check is in this store
				for (int col_index : col_used) {

					// Forward col_count in this store if not this col to be filtered
					if (!(col_count == this.rightFieldNo))
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

			// Find right to be compared col's values
			Object[] right_col_values = this.right_stores.get(store_index).columns.get(filter_col_index).entries;

			// Find right to be compared col's row indices
			Integer[] right_col_row_indices = right_cols[store_index].getAsInteger();

			// Get total number of cols
			int total_col_number = 0;

			// Initialize row indices holder for join
			ArrayList<ArrayList<Integer>> joined_row_indices = new ArrayList<>();


			for (int i = 0; i < left_cols.length; i += 1)
				joined_row_indices.add(new ArrayList<>());

			for (int i = 0; i < right_cols.length; i += 1)
				joined_row_indices.add(new ArrayList<>());

			int count = 0;
			// Conduct nested loop join for left and right table

			for (int i = 0; i < left_col_row_indices.length; i += 1) {

				for (int j = 0; j < right_col_row_indices.length; j += 1) {

					// Add combined tuple to joined row indices if matched
					if (left_col_values[left_col_row_indices[i]].equals(right_col_values[right_col_row_indices[j]])) {

						count += 1;
						// Add left part row indices
						for (int k = 0; k < left_cols.length; k += 1)
							joined_row_indices.get(k).add(left_cols[k].getAsInteger()[i]);

						// Add right part row indices
						for (int k = 0; k < right_cols.length; k += 1)
							joined_row_indices.get(left_cols.length + k).add(right_cols[k].getAsInteger()[j]);
					}
				}
			}

			// Convert joined_row_indices to DBColumn[]
			DBColumn[] returned_cols = new DBColumn[joined_row_indices.size()];

			for (int i = 0; i < joined_row_indices.size(); i += 1) {

				returned_cols[i] = new DBColumn(joined_row_indices.get(i).toArray(),
													DataType.INT);
			}

			return returned_cols;

		}

		// Build hash table of id of left child
		HashMap<Object, ArrayList<Integer>> hash_map = new HashMap<>();

		for (int i = 0; i < left_cols[this.leftFieldNo].entries.length; i += 1) {

			// Get current value
			Object key = left_cols[this.leftFieldNo].entries[i];

			// Add current index to hash table
			if (! hash_map.containsKey(key))
				hash_map.put(key, new ArrayList());

			hash_map.get(key).add(i);
		}

		// Initialize joined cols
		ArrayList<ArrayList<Object>> joined_values = new ArrayList<>(left_cols.length +
																	right_cols.length);
		for (int i = 0; i < left_cols.length + right_cols.length; i += 1)
			joined_values.add(new ArrayList<>());

		// Declare matched ids arraylist
		ArrayList<Integer> matched;

		// Loop through right cols to get matched
		for (int j = 0; j < right_cols[this.rightFieldNo].entries.length; j += 1) {

			// Get matching indices
			matched = hash_map.get(right_cols[this.rightFieldNo].entries[j]);

			// Append matched two records' entries into joined_values
			for (int k : matched) {

				// Loop through all cols in joined_values
				for (int l = 0; l < (left_cols.length + right_cols.length); l += 1){

					// Add left columns' entry
					if (l < left_cols.length)
						joined_values.get(l).add(left_cols[l].entries[k]);

					// Add right columns' entry
					else
						joined_values.get(l).add(right_cols[l - left_cols.length].entries[j]);
				}
			}
		}

		// Convert joined_values to columns
		ArrayList<DBColumn> joined_cols = new ArrayList<>();

		for (int i = 0; i < joined_values.size(); i += 1) {

			// Map col from left table to DBColumn
			if (i < left_cols.length)
				joined_cols.add(new DBColumn(joined_values.get(i).toArray(), left_cols[i].type));

			// Map col from right table to DBColumn
			else
				joined_cols.add(new DBColumn(joined_values.get(i).toArray(),
											right_cols[i - left_cols.length].type));

		}

		return joined_cols.toArray(new DBColumn[joined_cols.size()]);
	}
}
