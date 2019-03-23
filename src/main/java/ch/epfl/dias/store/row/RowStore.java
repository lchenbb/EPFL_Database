package ch.epfl.dias.store.row;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;

public class RowStore extends Store {

	// TODO: Add required structures

	// Private attributes

	// Page
	private ArrayList<DBTuple> rows;

	// Schema
	private DataType[] schema;

	// Filename
	private String filename;

	// Delimiter
	private String delimiter;

	public RowStore(DataType[] schema, String filename, String delimiter) {
		// TODO: Implement

		this.schema = schema;

		this.filename = filename;

		this.delimiter = delimiter;

		this.rows = new ArrayList<>();
	}

	@Override
	public void load() throws IOException {
		// TODO: Implement


		// Set up file reader
		FileReader fr;

		// Set up buffer reader
		BufferedReader br;
		try {

			// Load reader
			fr = new FileReader(filename);
			br = new BufferedReader(fr);

			String[] fields;
			String line;

			// Load each line
			while ((line = br.readLine()) != null){

				// Decompose line
				fields = line.split(delimiter);

				// Append fields to rows
				rows.add(new DBTuple(fields, schema));
			}

			// Add EOF row to the end
			rows.add(new DBTuple());

		} catch (IOException ex){

			// Log file not found error
			Logger.getLogger(RowStore.class.getName())
					.log(Level.SEVERE, null, ex);
		}

	}

	@Override
	public DBTuple getRow(int rownumber) {
		// TODO: Implement

		try {

			// Get required row
			return this.rows.get(rownumber);

		} catch (IndexOutOfBoundsException ex){

		}

		return null;
	}
}
