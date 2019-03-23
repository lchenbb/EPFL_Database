package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

import javax.xml.crypto.Data;

public class ProjectAggregate implements VolcanoOperator {

	// TODO: Add required structures

	public VolcanoOperator child;
	public Aggregate agg;
	public DataType dt;
	public int fieldNo;

	public ProjectAggregate(VolcanoOperator child, Aggregate agg, DataType dt, int fieldNo) {
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
	public DBTuple next() {
		// TODO: Implement

		// Initialize tuple holder as first tuple
		DBTuple current = this.child.next();

		if (current == null)
			return null;

		// Process count for every case
		if (this.agg == Aggregate.COUNT){

			int total = 1;

			while (this.child.next() != null)
				total += 1;

			return new DBTuple(new Object[]{total}, new DataType[]{this.dt});
		}

		switch (this.dt){

			case INT:

				switch (this.agg) {

					case MIN:
						// Initialize current value
						int int_current = current.getFieldAsInt(this.fieldNo);
						int int_next;

						while (true) {

							// Get next tuple from child
							current = this.child.next();

							// Return current value if no next
							if (current == null)
								return new DBTuple(new Object[]{int_current}, new DataType[]{this.dt});

							// Check whether need to update min
							int_next = current.getFieldAsInt(this.fieldNo);
							int_current = int_next > int_current ? int_current : int_next;
						}

					case MAX:

						// Initialize current value
						int int_current_max = current.getFieldAsInt(this.fieldNo);
						int int_next_max;

						// Loop
						while (true) {

							// Get next tuple from child
							current = this.child.next();

							// Return current value if no next
							if (current == null)
								return new DBTuple(new Object[]{int_current_max}, new DataType[]{this.dt});

							// Check whether need to update max
							int_next_max = current.getFieldAsInt(this.fieldNo);
							int_current_max = int_next_max < int_current_max ? int_current_max : int_next_max;
						}

					case AVG:

						// Initialize current value
						int int_avg = current.getFieldAsInt(this.fieldNo);
						int count = 1;

						// Loop
						while (true) {

							// Get next tuple from child
							current = this.child.next();

							// Return current value if no next
							if (current == null)
								return new DBTuple(new Object[]{(double) int_avg / count}, new DataType[]{this.dt});

							// Forward sum and count
							int_avg += current.getFieldAsInt(this.fieldNo);
							count += 1;
						}

					case SUM:

						// Initialize current value
						int int_sum = current.getFieldAsInt(this.fieldNo);

						// Loop
						while (true) {

							// Get next tuple
							current = this.child.next();

							// Return if null
							if (current == null)
								return new DBTuple(new Object[]{int_sum}, new DataType[]{this.dt});

							// Increment sum
							int_sum += current.getFieldAsInt(this.fieldNo);
						}

				}

			case DOUBLE:
				switch (this.agg){
					case MIN:
						// Initialize current value
						double double_current = current.getFieldAsDouble(this.fieldNo);
						double double_next;

						while (true) {

							// Get next tuple from child
							current = this.child.next();

							// Return current value if no next
							if (current == null)
								return new DBTuple(new Object[]{double_current}, new DataType[]{this.dt});

							// Check whether need to update min
							double_next = current.getFieldAsDouble(this.fieldNo);
							double_current = double_next > double_current ? double_current : double_next;
						}

					case MAX:

						// Initialize current value
						double double_current_max = current.getFieldAsDouble(this.fieldNo);
						double double_next_max;

						// Loop
						while (true) {

							// Get next tuple from child
							current = this.child.next();

							// Return current value if no next
							if (current == null)
								return new DBTuple(new Object[]{double_current_max}, new DataType[]{this.dt});

							// Check whether need to update max
							double_next_max = current.getFieldAsDouble(this.fieldNo);
							double_current_max = double_next_max < double_current_max ? double_current_max : double_next_max;
						}

					case AVG:

						// Initialize current value
						double double_avg = current.getFieldAsDouble(this.fieldNo);
						double count = 1;

						// Loop
						while (true) {

							// Get next tuple from child
							current = this.child.next();

							// Return current value if no next
							if (current == null)
								return new DBTuple(new Object[]{(double) double_avg / count}, new DataType[]{this.dt});

							// Forward sum and count
							double_avg += current.getFieldAsDouble(this.fieldNo);
							count += 1;
						}

						case SUM:

							// Initialize current value
							double double_sum = current.getFieldAsDouble(this.fieldNo);

							// Loop
							while (true) {

								// Get next tuple
								current = this.child.next();

								// Return if null
								if (current == null)
									return new DBTuple(new Object[]{double_sum}, new DataType[]{this.dt});

								// Increment sum
								double_sum += current.getFieldAsDouble(this.fieldNo);
							}
				}

		}
		return null;
	}

	@Override
	public void close() {
		// TODO: Implement
	}

}
