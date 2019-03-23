package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;
import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;

import java.io.IOException;

public class Scan implements VolcanoOperator {

	// TODO: Add required structures
	// Attribute
	private Store store;

	private int current_row;
	public Scan(Store store) {
		// TODO: Implement

		this.store = store;

		this.current_row = 0;
	}

	@Override
	public void open() {
		// TODO: Implement

		// Load store's data
		try {
			this.store.load();
		} catch (IOException ex){

		}
	}

	@Override
	public DBTuple next() {
		// TODO: Implement

		// Load next tuple
		DBTuple current = this.store.getRow(this.current_row);

		// Forward current row
		current_row += 1;

		if (current.eof)
			return null;

		return current;
	}

	@Override
	public void close() {
		// TODO: Implement

		this.store = null;
	}
}