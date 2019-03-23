package ch.epfl.dias.store.PAX;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.Buffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

public class PAXStore extends Store {

	// TODO: Add required structures

	// Attributes
	ArrayList<DBPAXpage> pages;

	DataType[] schema;

	String filename;

	String delimiter;

	int tuplePerPage;

	// Constructor
	public PAXStore(DataType[] schema, String filename, String delimiter, int tuplesPerPage) {
		// TODO: Implement

		this.schema = schema;

		this.filename = filename;

		this.delimiter = delimiter;

		this.tuplePerPage = tuplesPerPage;
	}

	@Override
	public void load() throws IOException {
		// TODO: Implement

		// Establish reader
		FileReader fr = new FileReader(this.filename);

		BufferedReader br = new BufferedReader(fr);

		// Define temperary storage of line
		String line;

		// Define temerary storage of data
		ArrayList<ArrayList<String>> data = new ArrayList<ArrayList<String>>();

		// Load data
		while ((line = br.readLine()) != null) {

			data.add(new ArrayList<String>(Arrays.asList(line.split(delimiter))));
		}

		// Get num of cols
		int col_num = data.get(0).size();

		// Set temporary storage of un-transposed data block
		ArrayList<ArrayList<String>> ori_block;
		// Set temporary storage of transposed data block
		ArrayList<ArrayList<String>> block = new ArrayList<ArrayList<String>>();
		// Define transposed block row container
		ArrayList<String> block_row = new ArrayList<String>();

		// Loop through data to get each block
		int upper_bound;
		for (int i = 0; i < data.size(); i += this.tuplePerPage) {

			// Get upperbound of copy
			upper_bound = Math.min(data.size(), i + this.tuplePerPage);

			// Copy
			ori_block = new ArrayList<ArrayList<String>>(data.subList(i, upper_bound));

			// Initialize transposed block row container
			block_row = new ArrayList<String>();

			// Transpose by populating block_row
			for (int j = 0; j < col_num; j += 1){

				// Get current col values
				for (int k = 0; k < ori_block.size(); k += 1){

					// Get the required value
					block_row.add(ori_block.get(k).get(j));
				}

				// Push to transposed block
				block.add(block_row);
			}

			// Construct DBPAXpage from block
			this.pages.add(new DBPAXpage(this.schema, block));

			block = new ArrayList<ArrayList<String>>();
		}
	}

	@Override
	public DBTuple getRow(int rownumber) {
		// TODO: Implement

		// Define page id to search
		int page_id = rownumber / this.tuplePerPage;

		// Return if page id invalid
		if (page_id >= this.pages.size())
			return null;

		// Calculate page offset
		int offset = rownumber - page_id * this.tuplePerPage;

		// Return if offset is invalid
		if (offset >= this.pages.get(page_id).row_count)
			return null;

		// Get current page
		DBPAXpage page = this.pages.get(page_id);

		// Collect the requested row
		String[] current = new String[page.schema.length];

		for (int i = 0; i < page.schema.length; i += 1) {

			current[i] = page.data.get(i).get(offset);
		}

		// Construct tuple
		return new DBTuple(current, page.schema);
	}
}
