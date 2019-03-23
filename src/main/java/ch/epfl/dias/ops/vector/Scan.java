package ch.epfl.dias.ops.vector;

import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.stream.IntStream;

public class Scan implements VectorOperator {

	// TODO: Add required structures

	private ColumnStore store;
	private int vectorsize;
	private DBColumn[] columns;
	private int current;
	private boolean eof;

	public Scan(Store store, int vectorsize) {
		// TODO: Implement

		// Initialization
		this.store = (ColumnStore) store;
		this.vectorsize = vectorsize;
		this.current = 0;
		this.eof = false;
	}
	
	@Override
	public void open() {
		// TODO: Implement

		// Ask store to load
		try {
			this.store.load();
		} catch (IOException ex){

		}

		// Get columns from store
		this.columns = this.store.getColumns(IntStream.range(0, this.store.col_num)
														.toArray());

	}

	@Override
	public DBColumn[] next() {
		// TODO: Implement

		// Return null if eof
		if (this.eof)
			return null;

		// Initialize holder for values to get
		ArrayList<ArrayList<Object>> next_values = new ArrayList<>();
		for (int i = 0; i < this.columns.length; i += 1)
			next_values.add(new ArrayList<>());

		// Loop through next bunch
		for (int i = this.current;
			 i < ((this.current + this.vectorsize) < this.columns[0].entries.length ?
					 this.current + this.vectorsize :
					 this.columns[0]
							 .entries
							 .length); i += 1) {

			// Loop through all columns
			for (int k = 0; k < this.columns.length; k += 1)
				next_values.get(k).add(this.columns[k].entries[i]);
		}

		// Update current index
		this.current += this.vectorsize;

		// Check for update eof
		if (this.current >= this.columns[0].entries.length)
			this.eof = true;

		// Initialize DBColumns holder
		ArrayList<DBColumn> next_cols = new ArrayList<>();

		// Populate DBColumns holder
		for (int i = 0; i < this.columns.length; i += 1) {
			// Add col to next_cols
			next_cols.add(new DBColumn(next_values.get(i).toArray(),
										this.columns[i].type));
		}

		return next_cols.toArray(new DBColumn[next_cols.size()]);
	}

	@Override
	public void close() {
		// TODO: Implement
	}
}
