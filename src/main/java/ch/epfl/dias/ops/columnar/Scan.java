package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;
import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

public class Scan implements ColumnarOperator {

	// TODO: Add required structures

	// Store of data
	private ColumnStore store;

	// Indicator of late materialization
	private boolean is_late;

	public Scan(ColumnStore stores) {
		// TODO: Implement

		// Initialization
		this.store = stores;
		this.is_late = this.store.lateMaterialization;

	}

	@Override
	public ArrayList<ColumnStore> get_store() {

		// Return self's store
		return new ArrayList<>(Arrays.asList(this.store));
	}

	@Override
	public boolean is_late_materialization() {

		return this.is_late;
	}
	@Override
	public DBColumn[] execute() {
		// TODO: Implement

		try {
			// Load data in
			this.store.load();

			// Return all columns if not late materialization
			if (!this.is_late)
				return this.store.getColumns(IntStream.range(0, this.store.col_num).toArray());

			// Return a DBColumn containing the ids
			else
				return new DBColumn[]{new DBColumn(IntStream.range(0, this.store.getColumns(new int[]{0})[0].entries.length)
															.mapToObj(i -> i).toArray(),
													DataType.INT)};

		} catch (IOException ex){

		}
		return null;
	}
}
