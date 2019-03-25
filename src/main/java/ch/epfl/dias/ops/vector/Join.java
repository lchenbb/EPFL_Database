package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import java.util.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class Join implements VectorOperator {

	// TODO: Add required structures

	private VectorOperator leftChild;
	private VectorOperator rightChild;
	private int leftFieldNo;
	private int rightFieldNo;
	private HashMap<Object, ArrayList<Integer>> hash_map;
	private ArrayList<DBColumn> left_cols;

	public Join(VectorOperator leftChild, VectorOperator rightChild, int leftFieldNo, int rightFieldNo) {
		// TODO: Implement

		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.leftFieldNo = leftFieldNo;
		this.rightFieldNo = rightFieldNo;
		this.hash_map = new HashMap<>();
		this.left_cols = new ArrayList<>();
	}

	@Override
	public void open() {
		// TODO: Implement

		// Ask left and right child to open
		this.leftChild.open();
		this.rightChild.open();

		// Build hash map from left child
		DBColumn[] left_next;
		int count = 0;

		// Set flag of initialization of left col
		boolean left_cols_initialized = false;

		// Pull every bunch in
		while ((left_next = this.leftChild.next()) != null) {

			// Update hash map for this bunch
			for (int i = 0; i < left_next[0].entries.length; i += 1) {

				// Check whether need to add new key to hash map
				if (!hash_map.containsKey(left_next[this.leftFieldNo].entries[i]))
					this.hash_map.put(left_next[this.leftFieldNo].entries[i],
										new ArrayList<>());

				this.hash_map.get(left_next[this.leftFieldNo].entries[i])
								.add(count + i);
			}

			// Update count
			count += left_next[0].entries.length;

			// Store this bunch in left_cols
			for (int i = 0; i < left_next.length; i += 1) {

				// Initialize left cols to be null
				if (!left_cols_initialized)
					this.left_cols.add(null);

				this.left_cols.set(i, this.merge_col(this.left_cols.get(i),
													left_next[i]));
			}

			// Set left cols_initialized to be true
			left_cols_initialized = true;
		}
	}

	@Override
	public DBColumn[] next() {
		// TODO: Implement

		// Declare right next bunch container
		DBColumn[] right_next;

		// Initialize buffer for joined tuple
		ArrayList<DBColumn> buffer = new ArrayList<>();

		// Initialize value buffer for joined tuple
		ArrayList<ArrayList<Object>> val_buffer = new ArrayList<>();

		// Pull next bunch from right child
		right_next = this.rightChild.next();

		// Return null if null returned
		if (right_next == null)
			return null;

		// Populate value buffer with empty array list
		for (int i = 0; i < this.left_cols.size() + right_next.length; i += 1)
			val_buffer.add(new ArrayList<>());

		// Check for matched tuples

		// Declare matched found flag
		boolean filled;

		// Declare buffer to store left matched tuples' id
		ArrayList<Integer> left_matched_id;

		while (right_next != null) {

			// Set no matched detected
			filled = false;

			// Loop through every returned right tuple
			for (int i = 0; i < right_next[0].entries.length; i += 1) {

				// Get matched left ids
				left_matched_id = this.hash_map.get(right_next[this.rightFieldNo].entries[i]);

				// Continue if no matched found
				if (left_matched_id == null)
					continue;

				// Set matched detected
				filled = true;

				// Push matched tuples' values into val buffer
				for (int j : left_matched_id) {

					// Add left tuple's values
					for (int k = 0; k < this.left_cols.size(); k += 1)
						val_buffer.get(k).add(this.left_cols.get(k).entries[j]);

					// Add right tuple's values
					for (int k = 0; k < right_next.length; k += 1)
						val_buffer.get(k + this.left_cols.size()).add(right_next[k].entries[i]);
				}
			}

			// Return matched tuples' cols if matched detected
			if (filled) {

				for (int i = 0; i < val_buffer.size(); i += 1) {

					// Build DBColumn for values in left col
					if (i < left_cols.size())
						buffer.add(new DBColumn(val_buffer.get(i).toArray(),
												left_cols.get(i).type));

					// Build DBColumn for values in right col
					else
						buffer.add(new DBColumn(val_buffer.get(i).toArray(),
												right_next[i - left_cols.size()].type));
				}

				return buffer.toArray(new DBColumn[buffer.size()]);
			}

			// Pull next bunch of right
			right_next = this.rightChild.next();
		}
		return null;
	}

	@Override
	public void close() {
		// TODO: Implement

		// Ask child to close
		this.leftChild.close();
		this.rightChild.close();

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
