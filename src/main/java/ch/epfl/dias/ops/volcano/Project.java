package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Project implements VolcanoOperator {

	// TODO: Add required structures

	// Declare class variable
	public VolcanoOperator child;
	public int[] fieldNo;

	public Project(VolcanoOperator child, int[] fieldNo) {
		// TODO: Implement

		// Initialize child
		this.child = child;

		// Initialize fieldNo
		this.fieldNo = fieldNo;
	}

	@Override
	public void open() {
		// TODO: Implement

		// Ask child to open
		this.child.open();
	}

	@Override
	public DBTuple next() {
		// TODO: Implement

		// Ask child to get next
		DBTuple next = this.child.next();

		// Initialize projection types
		ArrayList<DataType> types = new ArrayList<>();

		// Initialize projection fields
		ArrayList<Object> fields = new ArrayList<>();

		// Populate projection fields and types
		for (int i : this.fieldNo) {

			types.add(next.types[i]);
			fields.add(next.fields[i]);
		}

		// Initialize return tuple
		DBTuple this_next = new DBTuple(fields.toArray(),
										types.toArray(new DataType[types.size()]));

		return this_next;

	}

	@Override
	public void close() {
		// TODO: Implement

		// Ask child to close
		this.child.close();
	}
}
