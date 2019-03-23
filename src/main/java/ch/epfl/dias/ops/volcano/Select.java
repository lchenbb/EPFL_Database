package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.row.DBTuple;

public class Select implements VolcanoOperator {

	// TODO: Add required structures

	// Attributes
	VolcanoOperator child;

	BinaryOp op;

	int field_num;

	int value;

	public Select(VolcanoOperator child, BinaryOp op, int fieldNo, int value) {
		// TODO: Implement

		this.child = child;

		this.op = op;

		this.field_num = fieldNo;

		this.value = value;
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

		while (true) {

			// Get input from child
			DBTuple input = this.child.next();

			// Return null if input is null
			if (input == null)
				return input;


			// Get value interested
			int compared_value = input.getFieldAsInt(this.field_num);

			switch(this.op){

				case EQ:

					if (compared_value == this.value)
						return input;
					continue;

				case GE:

					if (compared_value >= this.value)
						return input;
					continue;

				case GT:

					if (compared_value > this.value)
						return input;
					continue;

				case LE:

					if (compared_value <= this.value)
						return input;
					continue;

				case LT:

					if (compared_value < this.value)
						return input;
					continue;

				default:

					// Compare not eq
					if (compared_value != this.value)
						return input;
					continue;
			}

		}
	}

	@Override
	public void close() {
		// TODO: Implement
		this.child.close();
	}
}
