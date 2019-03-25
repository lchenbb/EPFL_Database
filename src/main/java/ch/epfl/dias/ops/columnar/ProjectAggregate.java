package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;
import org.omg.CORBA.OBJ_ADAPTER;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

public class ProjectAggregate implements ColumnarOperator {

	// TODO: Add required structures

	private ColumnarOperator child;
	private Aggregate agg;
	private DataType dt;
	private int fieldNo;
	private ArrayList<ColumnStore> stores;
	private boolean is_late;

	@Override
	public boolean is_late_materialization() {

		return this.is_late;
	}

	@Override
	public ArrayList<ColumnStore> get_store() {

		return this.stores;
	}

	public ProjectAggregate(ColumnarOperator child, Aggregate agg, DataType dt, int fieldNo) {
		// TODO: Implement

		this.child = child;
		this.agg = agg;
		this.dt = dt;
		this.fieldNo = fieldNo;

		this.is_late = this.child.is_late_materialization();
	}

	@Override
	public DBColumn[] execute() {
		// TODO: Implement

		// Ask child to output data
		DBColumn[] child_out = this.child.execute();

		// Get child's store
		this.stores = this.child.get_store();

		// Handle late materialization
		if (this.is_late) {

			// Return null if child out is null
			if (child_out == null)
				return null;

			// Get the col to aggregate

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

			while (!localization_end) {

				// Load current store's col_used
				col_used = this.stores.get(store_index).cols_used;

				// Check whether field to check is in this store
				for (int col_index : col_used) {

					// Forward col_count in this store if not this col to be filtered
					if (!(col_count == this.fieldNo))
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
			}

			// Get col to aggregate
			DBColumn target_col = this.stores.get(store_index).columns.get(filter_col_index);

			List<Object> values = new ArrayList<>();

			// Get target col's values
			for (int x : child_out[store_index].getAsInteger()) {

				values.add(target_col.entries[x]);
			}

			// Handle count
			if (this.agg == Aggregate.COUNT) {

				return new DBColumn[]{new DBColumn(new Object[]{values.size()},
						DataType.INT)};

			}

			// Handle arithmetic opeation
			switch (this.dt) {

				case INT:

					Integer[] int_values = values.stream()
													.mapToInt(i -> Integer.valueOf(i.toString()))
													.boxed()
													.toArray(Integer[]::new);

					switch (this.agg) {

						case MIN:

							int int_min = Arrays.stream(int_values).mapToInt(i -> i).min().getAsInt();

							return new DBColumn[]{new DBColumn(new Object[]{int_min},
									DataType.INT)};

						case MAX:

							int int_max = Arrays.stream(int_values).mapToInt(i -> i).max().getAsInt();

							return new DBColumn[]{new DBColumn(new Object[]{int_max},
									DataType.INT)};

						case SUM:

							int int_sum = Arrays.stream(int_values).mapToInt(i -> i).sum();

							return new DBColumn[]{new DBColumn(new Object[]{int_sum},
									DataType.INT)};

						case AVG:

							double int_avg = Arrays.stream(int_values).mapToInt(i -> i).average().getAsDouble();

							return new DBColumn[]{new DBColumn(new Object[]{int_avg},
																DataType.DOUBLE)};

					}

				case DOUBLE:

					Double[] double_values = values.stream()
													.mapToDouble(i -> Double.valueOf(i.toString()))
													.boxed()
													.toArray(Double[]::new);

					switch (this.agg) {

						case MIN:

							double double_min = Arrays.stream(double_values).mapToDouble(i -> i).min().getAsDouble();

							return new DBColumn[]{new DBColumn(new Object[]{double_min},
									DataType.DOUBLE)};

						case MAX:

							double double_max = Arrays.stream(double_values).mapToDouble(i -> i).max().getAsDouble();

							return new DBColumn[]{new DBColumn(new Object[]{double_max},
									DataType.DOUBLE)};

						case SUM:

							double double_sum = Arrays.stream(double_values).mapToDouble(i -> i).sum();

							return new DBColumn[]{new DBColumn(new Object[]{double_sum},
									DataType.DOUBLE)};

						case AVG:

							double double_avg = Arrays.stream(double_values).mapToDouble(i -> i).average().getAsDouble();

							return new DBColumn[]{new DBColumn(new Object[]{double_avg},
													DataType.DOUBLE)};
					}
			}
		}

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
