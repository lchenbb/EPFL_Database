package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;

import javax.xml.crypto.Data;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HashJoin implements VolcanoOperator {

	// TODO: Add required structures

	// Declare member
	public VolcanoOperator leftChild;
	public VolcanoOperator rightChild;
	public int leftFieldNo;
	public int rightFieldNo;
	public HashMap<Object, ArrayList<DBTuple>> hash;
	public boolean built;
	public ArrayList<DBTuple> buffer;

	public HashJoin(VolcanoOperator leftChild, VolcanoOperator rightChild, int leftFieldNo, int rightFieldNo) {
		// TODO: Implement

		// Initialize member
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.leftFieldNo = leftFieldNo;
		this.rightFieldNo = rightFieldNo;
		this.hash = new HashMap<>();
		this.built = false;
		this.buffer = new ArrayList<>();
	}

	@Override
	public void open() {
		// TODO: Implement

		// Ask child to open
		this.leftChild.open();
		this.rightChild.open();

		// Build hash table of left child
		if (!this.built) {
			// Ask left child to get all tuples
			ArrayList<DBTuple> left = new ArrayList<>();
			DBTuple current;

			while (true) {

				// Collect next
				current = this.leftChild.next();

				// Check whether next is not null
				if (current != null) {

					left.add(current);
					continue;
				}

				break;
			}

			if (left.size() == 0)
				return;

			// Build hash table from the left
			left.stream()
					.map(tuple -> {
						Object field = tuple.fields[this.leftFieldNo];

						boolean temp = this.hash.containsKey(field);
						if (!this.hash.containsKey(field))
							this.hash.put(field, new ArrayList());
						this.hash.get(field).add(new DBTuple(new Object[]{1}, new DataType[]{DataType.INT}));
						return 0;
					}).collect(Collectors.toList());

			// Set built to true
			this.built = true;
		}
	}

	@Override
	public DBTuple next() {
		// TODO: Implement

		// Pop buffer if buffer is not empty
		if (this.buffer.size() > 0)
			return this.buffer.remove(this.buffer.size() - 1);

		DBTuple right_next;
		while (true) {
			// Ask right child to get next element
			right_next = this.rightChild.next();

			// Return null if no next obtained
			if (right_next == null)
				return null;

			// Search for correspondance in hash table
			ArrayList<DBTuple> matched = this.hash.get(right_next.fields[this.rightFieldNo]);

			// Push matched to buffer if not empty
			if (matched.size() > 0) {

				// Combine right with left
				for (int i = 0; i < matched.size(); i += 1)
					matched.set(i, this.combine(matched.get(i), right_next));

				// Push to buffer
				this.buffer.addAll(matched);

				// Pop buffer
				return this.buffer.remove(this.buffer.size() - 1);
			}
			else
				continue;
		}

	}

	/** Combine left and right operand of DBTuples
	 *
	 * @param left
	 * @param right
	 * @return combined tuple
	 */
	private DBTuple combine(DBTuple left, DBTuple right){

		// Concatenate types
		ArrayList<DataType> types = new ArrayList<>(Arrays.asList(left.types));
		types.addAll(new ArrayList<>(Arrays.asList(right.types)));

		// Concatenate fields
		ArrayList<Object> fields = new ArrayList<>(Arrays.asList(left.fields));
		fields.addAll(new ArrayList<>(Arrays.asList(right.fields)));

		return new DBTuple(fields.toArray(), types.toArray(new DataType[types.size()]));

	}
	@Override
	public void close() {
		// TODO: Implement

		// Ask child to close
		this.leftChild.close();
		this.rightChild.close();
	}
}
