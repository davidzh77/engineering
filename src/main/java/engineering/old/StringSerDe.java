/**
replace all the N/A to null
*/
package engineering.old;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class StringSerDe implements SerDe {

    
    /**
     * The number of columns in the table this SerDe is being used with
     */
    private int numColumns;
    /**
     * List of column names in the table
     */
    private List<String> columnNames;
    /**
     * An ObjectInspector to be used as meta-data about a deserialized row
     */
    private StructObjectInspector rowOI;
    /**
     * List of row objects
     */
    private ArrayList<Object> row;
    /**
     * List of column type information
     */
    private List<TypeInfo> columnTypes;
    Object[] outputFields;
    Text outputRowText;
    
	@Override
	public void initialize(Configuration conf, Properties tblProps)
			throws SerDeException {
		
		  String columnNameProperty = tblProps.getProperty(serdeConstants.LIST_COLUMNS);
		  String columnTypeProperty = tblProps.getProperty(serdeConstants.LIST_COLUMN_TYPES);
		  
		    List<String> columnNames = Arrays.asList(columnNameProperty.split(","));
		    List<TypeInfo> columnTypes = TypeInfoUtils
		        .getTypeInfosFromTypeString(columnTypeProperty);
        		    
		  assert columnNames.size() == columnTypes.size();
		  numColumns = columnNames.size();
		    
		  List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(
			        columnNames.size());
			    for (int c = 0; c < numColumns; c++) {
			      columnOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
			    }
			    // StandardStruct uses ArrayList to store the row.
			    rowOI = ObjectInspectorFactory.getStandardStructObjectInspector(
			        columnNames, columnOIs);
			    
        // Create an empty row object to be reused during deserialization
        row = new ArrayList<Object>(numColumns);
        for (int c = 0; c < numColumns; c++) {
                row.add(null);
        }

        outputFields = new Object[numColumns];
        outputRowText = new Text();
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.hive.serde2.Deserializer#deserialize(org.apache.hadoop.io.Writable)
	 */
	@Override
	public Object deserialize(Writable blob) throws SerDeException {
        Text rowText = (Text) blob;
        String[] a = rowText.toString().split("|");

        for (int c = 0; c < numColumns; c++) {

          row.add(a[c].replaceAll("N/A", ""));
         }
         
        
        return row;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.hive.serde2.Deserializer#getObjectInspector()
	 */
	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		 return rowOI;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.hive.serde2.Deserializer#getSerDeStats()
	 */
	@Override
	public SerDeStats getSerDeStats() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.hive.serde2.Serializer#getSerializedClass()
	 */
	@Override
	public Class<? extends Writable> getSerializedClass() {
		// TODO Auto-generated method stub
		 return Text.class;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.hive.serde2.Serializer#serialize(java.lang.Object, org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector)
	 */
	@Override
	public Writable serialize(Object obj, ObjectInspector objInspector)
			throws SerDeException {
		return null;
	}
	
}