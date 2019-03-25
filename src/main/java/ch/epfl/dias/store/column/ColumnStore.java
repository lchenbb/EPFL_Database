package ch.epfl.dias.store.column;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;

public class ColumnStore extends Store {

	// TODO: Add required structures

	// Attributes
	public ArrayList<DBColumn> columns;

	public DataType[] schema;

	public String filename;

	public String delimiter;

	public List<Integer> cols_used;

	public boolean lateMaterialization;

	public Integer col_num;

	public ColumnStore(DataType[] schema, String filename, String delimiter) {
		this(schema, filename, delimiter, false);
	}

	public ColumnStore(DataType[] schema, String filename, String delimiter, boolean lateMaterialization) {
		// TODO: Implement

		this.schema = schema;

		this.filename = filename;

		this.delimiter = delimiter;

		this.lateMaterialization = lateMaterialization;

		this.col_num = schema.length;

		this.cols_used = IntStream.range(0, this.col_num).boxed().collect(Collectors.toList());
	}

	@Override
	public void load() throws IOException {
		// TODO: Implement

		try {
			// Check whether file exist
			if (!(new File(this.filename)).exists())
				return;

			// Initialize data holder
			ArrayList<ArrayList<String>> data = new ArrayList<ArrayList<String>>();

			// Initialize reader
			FileReader fr = new FileReader(this.filename);

			BufferedReader br = new BufferedReader(fr);

			// Load data
			boolean first = true;

			String line;
			String[] fields;

			while ((line = br.readLine()) != null) {

				// Get fields
				fields = line.split(delimiter);

				// Append field to list for corresponding column
				for (int i = 0; i < fields.length; i ++){

					if (first) {

						// Create new list for column
						data.add(new ArrayList<String>());

						// Add data into column
						data.get(i).add(fields[i]);
					} else {

						// Add data into column
						data.get(i).add(fields[i]);
					}
				}

				// Finish first row for every col
				first = false;
			}

			// Construct DBColumn for each column
			columns = IntStream.range(0, data.size())
								.mapToObj(n -> new DBColumn(data.get(n).toArray(), this.schema[n]))
								.collect(Collectors.toCollection(ArrayList::new));

			// Add EOF column to column
			columns.add(new DBColumn());

		} catch (IOException ex){

			Logger.getLogger(ColumnStore.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	@Override
	public DBColumn[] getColumns(int[] columnsToGet) {
		// TODO: Implement

		// Check whether index gets out of bound
		if (Arrays.stream(columnsToGet).max().getAsInt() >= this.col_num) {
			Logger.getLogger(ColumnStore.class.getName()).log(Level.SEVERE, "Try to get col not exist!!!");
			return null;
		}

		// Get the required cols in a new list
		ArrayList<DBColumn> required_cols = new ArrayList<DBColumn>();

		for (int col : columnsToGet){

			required_cols.add(this.columns.get(col));
		}

		// Return the cols as array
		return required_cols.toArray(new DBColumn[required_cols.size()]);
	}
}
