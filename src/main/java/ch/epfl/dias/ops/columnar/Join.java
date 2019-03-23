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
	private ArrayList<ColumnStore> left_store;
	private ArrayList<ColumnStore> right_store;
	private boolean is_late;

	@Override
	public ArrayList<ColumnStore> get_store() {

		// Return the combination of left and right store
		return new ArrayList<>(Stream.concat(this.left_store.stream(),
												this.right_store.stream())
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
		this.left_store = this.leftChild.get_store();
		this.right_store = this.rightChild.get_store();
	}

	public DBColumn[] execute() {
		// TODO: Implement

		// Ask left child to get data
		DBColumn[] left_cols = this.leftChild.execute();

		// Ask right child to get data
		DBColumn[] right_cols = this.rightChild.execute();

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
