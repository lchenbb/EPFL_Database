package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Project implements ColumnarOperator {

	// TODO: Add required structures

	private ColumnarOperator child;
	private int[] columns;
	private ArrayList<ColumnStore> stores;
	private boolean is_late;

	@Override
	public ArrayList<ColumnStore> get_store() {

		return this.stores;
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
	}

	public DBColumn[] execute() {
		// TODO: Implement

		// Ask child to execute
		DBColumn[] column_values = this.child.execute();

		// Return required cols
		if (column_values == null)
			return null;

		// Get store of child
		this.stores = this.child.get_store();


		// Modify col index in stores if late materialization
		if (this.is_late) {

			// Build total column indices and store indices

			// Initialize total col indices container
			List<Integer> total_col_indices = new ArrayList<>();

			// Initialize total store indices container
			List<Integer> total_store_indices = new ArrayList<>();

			for (int i = 0; i < this.stores.size(); i += 1) {

				// Add current store indices n times into total_store_indices
				// where n is length of its col used
				total_store_indices.addAll(Collections.nCopies(this.stores.get(i).cols_used.size(),
																i));

				// Add current store's col used to total_col_indices
				total_col_indices.addAll(this.stores.get(i).cols_used);
			}

			// Filter the stores and cols

			// Initialize filtered store-col map
			HashMap<Integer, List<Integer>> store_col = new HashMap<>();

			// Populate store-col map by finding the projected cols
			Integer current_store;
			for (Integer index : this.columns) {

				// Get current store
				current_store = total_store_indices.get(index);

				// Add current store to key if current store not in map
				if (!store_col.containsKey(current_store))
					store_col.put(current_store, new ArrayList<>());

				// Push current col to map's corresponding value
				store_col.get(current_store).add(total_col_indices.get(index));
			}

			// Initialize DBColumn[] to return
			ArrayList<DBColumn> return_dbcols = new ArrayList<>();

			// Filter stores
			ArrayList<Integer> sorted_filtered_stores = new ArrayList<>(store_col.keySet());
			Collections.sort(sorted_filtered_stores);

			for (Integer store_index : sorted_filtered_stores) {

				// Add current store's row indices to return dbcols
				return_dbcols.add(column_values[store_index]);

				// Update current store's col used
				this.stores.get(store_index).cols_used = store_col.get(store_index);

				// Update current store's col num
				this.stores.get(store_index).col_num = store_col.get(store_index).size();
			}

			// Remove the stores that are not selected
			// Notice that it requires upper layer nodes to get
			// child's stores after execution
			for (int i = 0; i < this.stores.size(); i += 1) {

				if (!sorted_filtered_stores.contains(i))
					this.stores.remove(i);
			}

			// Return new rows dbcol array
			return return_dbcols.toArray(new DBColumn[return_dbcols.size()]);
		}

		// Initialize projection cols
		ArrayList<DBColumn> proj_cols = new ArrayList();

		// Populate proj_cols
		for (int i : columns)
			proj_cols.add(column_values[i]);

		// Return proj cols
		return proj_cols.toArray(new DBColumn[proj_cols.size()]);

	}
}
