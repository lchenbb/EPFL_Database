package ch.epfl.dias.store.PAX;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import java.util.ArrayList;

public class DBPAXpage {

	// TODO:

    // Attribute
    public DataType[] schema;

    public ArrayList<ArrayList<String>> data;

    public int row_count;

    public boolean eof = false;

    // End of page Constructor (Useless because EOF should be defined for row)
    public DBPAXpage(){

        this.eof = true;
    }

    /** Parameterized constructor
     *
     * @param schema
     * @param data list of cols
     */
    public DBPAXpage(DataType[] schema, ArrayList<ArrayList<String>> data){

        this.schema = schema;

        this.data = data;

        // Define row count to avoid invalid accessing
        this.row_count = data.get(0).size();
    }

}
