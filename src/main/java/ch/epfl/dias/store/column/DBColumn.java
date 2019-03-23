package ch.epfl.dias.store.column;

import java.util.ArrayList;
import java.util.Arrays;

import ch.epfl.dias.store.DataType;

public class DBColumn {

	// TODO: Implement

	// Attributes
	public Object[] entries;

	public DataType type;

	public boolean eof;

	// Constructor
	public DBColumn(Object[] entries, DataType type){

		this.entries = entries;

		this.type = type;

		this.eof = false;
	}

	public DBColumn(){

		this.eof = true;
	}

	// Helper functions
	public Integer[] getAsInteger() {
		// TODO: Implement

		return Arrays.stream(this.entries)
						.map(Object::toString)
						.map(Integer::valueOf)
						.toArray(Integer[]::new);
	}

	public Double[] getAsDouble(){

		return Arrays.stream(this.entries)
						.map(Object::toString)
						.map(Double::valueOf)
						.toArray(Double[]::new);
	}

	public Boolean[] getAsBoolean(){

		return Arrays.stream(this.entries)
						.map(x -> Boolean.valueOf(x.toString()))
						.toArray(Boolean[]::new);
	}

	public String[] getAsString(){

		return Arrays.stream(this.entries)
						.map(x -> x.toString())
						.toArray(String[]::new);
	}
}
