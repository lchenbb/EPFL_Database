package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

public class ProjectAggregate implements ColumnarOperator {

	// TODO: Add required structures

	private ColumnarOperator child;
	private Aggregate agg;
	private DataType dt;
	private int fieldNo;
	private ArrayList<ColumnStore> store;
	private boolean is_late;

	@Override
	public boolean is_late_materialization() {

		return this.is_late;
	}

	@Override
	public ArrayList<ColumnStore> get_store() {

		return this.store;
	}

	public ProjectAggregate(ColumnarOperator child, Aggregate agg, DataType dt, int fieldNo) {
		// TODO: Implement

		this.child = child;
		this.agg = agg;
		this.dt = dt;
		this.fieldNo = fieldNo;

		this.store = this.child.get_store();
		this.is_late = this.child.is_late_materialization();
	}

	@Override
	public DBColumn[] execute() {
		// TODO: Implement

		// Ask child to output data
		DBColumn[] child_out = this.child.execute();

		// Conduct count for all dt
		if (this.agg == Aggregate.COUNT) {

			return new DBColumn[]{new DBColumn(new Object[]{child_out == null ? 0
																			: child_out[fieldNo].entries.length},
												DataType.INT)};
		}

		// Conduct remaining operation for double and int
		switch (this.dt) {

			case INT:

				// Return null if child_out is null
				if (child_out == null)
					return null;

				Integer[] int_values = child_out[this.fieldNo].getAsInteger();
				switch (this.agg) {

					case MAX:

						return new DBColumn[]{new DBColumn(new Object[]{Arrays.stream(int_values)
																				.max(Integer::compare)},
															DataType.INT)};

					case MIN:

						return new DBColumn[]{new DBColumn(new Object[]{Arrays.stream(int_values)
																				.min(Integer::compare)},
															DataType.INT)};

					case SUM:

						// Calculate sum
						Integer sum = Arrays.stream(int_values).mapToInt(i -> i).sum();

						return new DBColumn[]{new DBColumn(new Object[]{sum}, DataType.INT)};

					case AVG:

						// Calculate avg
						Double avg = Arrays.stream(int_values).mapToInt(i -> i).average().getAsDouble();

						return new DBColumn[]{new DBColumn(new Object[]{avg}, DataType.DOUBLE)};
				}


			case DOUBLE:

				// Return null if child_out is null
				if (child_out == null)
					return null;

				Double[] double_values = child_out[this.fieldNo].getAsDouble();
				switch (this.agg) {

					case MAX:

						return new DBColumn[]{new DBColumn(new Object[]{Arrays.stream(double_values)
								.max(Double::compare)},
								DataType.DOUBLE)};

					case MIN:

						return new DBColumn[]{new DBColumn(new Object[]{Arrays.stream(double_values)
								.min(Double::compare)},
								DataType.DOUBLE)};

					case SUM:

						// Calculate sum
						Double sum = Arrays.stream(double_values).mapToDouble(i -> i).sum();

						return new DBColumn[]{new DBColumn(new Object[]{sum}, DataType.DOUBLE)};

					case AVG:

						// Calculate avg
						Double avg = Arrays.stream(double_values).mapToDouble(i -> i).average().getAsDouble();

						return new DBColumn[]{new DBColumn(new Object[]{avg}, DataType.DOUBLE)};
				}

		}

		return null;
	}
}
