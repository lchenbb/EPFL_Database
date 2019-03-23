package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

public class ProjectAggregate implements VectorOperator {

	// TODO: Add required structures

	private VectorOperator child;
	private Aggregate agg;
	private DataType dt;
	private int fieldNo;

	public ProjectAggregate(VectorOperator child, Aggregate agg, DataType dt, int fieldNo) {
		// TODO: Implement

		this.child = child;
		this.agg = agg;
		this.dt = dt;
		this.fieldNo = fieldNo;

	}

	@Override
	public void open() {
		// TODO: Implement

		// Ask child to open
		this.child.open();
	}

	@Override
	public DBColumn[] next() {
		// TODO: Implement

		// Define holder of cols
		ArrayList<DBColumn> cols = new ArrayList<>();

		// Declare holder for next bunch of the child
		DBColumn[] child_next;

		// Pull all the data from the child
		int d = 0;
		while((child_next = this.child.next()) != null) {

			// Check whether need to initialize DBColumns in the cols
			if (cols.size() == 0)
				for (int i = 0; i < child_next.length; i += 1)
					cols.add(null);

			// Merge new bunch with previous bunches
			for (int i = 0; i < child_next.length; i += 1) {
				cols.set(i, this.merge_col(i < cols.size() ? cols.get(i) : null, child_next[i]));
			}
			d += 1;
		}

		// Handle count for all datatypes
		if (this.agg == Aggregate.COUNT)
			return new DBColumn[]{new DBColumn(new Object[]{cols.get(0) == null ? 0 : cols.get(0)
																							.entries.length},
												DataType.INT)};

		// Return null if child_next is null
		if (cols == null || cols.get(0).entries.length == 0)
			return null;

		// Deal with arithmetic operations for double and int
		switch (this.dt) {

			case INT:

				// Collect the aggregated col
				Integer[] int_values = cols.get(this.fieldNo).getAsInteger();

				switch (this.agg){

					case SUM:

						int int_sum = (Arrays.stream(int_values).mapToInt(i -> i)).sum();

						return new DBColumn[]{new DBColumn(new Object[]{int_sum},
															DataType.INT)};

					case MIN:

						int int_min = (Arrays.stream(int_values).mapToInt(i -> i)).min().getAsInt();

						return new DBColumn[]{new DBColumn(new Object[]{int_min},
															DataType.INT)};
					case MAX:

						int int_max = Arrays.stream(int_values).mapToInt(i -> i).max().getAsInt();

						return new DBColumn[]{new DBColumn(new Object[]{int_max},
								DataType.INT)};

					case AVG:

						double int_avg = Arrays.stream(int_values).mapToInt(i -> i).average().getAsDouble();

						return new DBColumn[]{new DBColumn(new Object[]{int_avg},
															DataType.DOUBLE)};

				}

				break;
			case DOUBLE:

				// Collect the aggregated col
				Double[] double_values = cols.get(this.fieldNo).getAsDouble();

				switch (this.agg){

					case SUM:

						double double_sum = Arrays.stream(double_values).mapToDouble(i -> i).sum();

						return new DBColumn[]{new DBColumn(new Object[]{double_sum},
								DataType.DOUBLE)};

					case MIN:

						double double_min = Arrays.stream(double_values).mapToDouble(i -> i).min().getAsDouble();

						return new DBColumn[]{new DBColumn(new Object[]{double_min},
								DataType.DOUBLE)};
					case MAX:

						double double_max = Arrays.stream(double_values).mapToDouble(i -> i).max().getAsDouble();

						return new DBColumn[]{new DBColumn(new Object[]{double_max},
								DataType.DOUBLE)};

					case AVG:

						double double_avg = Arrays.stream(double_values).mapToDouble(i -> i).average().getAsDouble();

						return new DBColumn[]{new DBColumn(new Object[]{double_avg},
								DataType.DOUBLE)};

				}
				break;
		}
		return null;
	}

	@Override
	public void close() {
		// TODO: Implement
		this.child.close();
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
