/**
https://code.google.com/p/hive-json-serde/source/browse/trunk/src/org/apache/hadoop/hive/contrib/serde2/JsonSerde.java
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
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class JsonSerde1 implements SerDe {

	  /**
     * Apache commons logger
     */
    private static final Log LOG =
    		LogFactory.getLog(JsonSerde1.class.getName());
    
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
    
	@Override
	public void initialize(Configuration conf, Properties tblProps)
			throws SerDeException {
		LOG.debug("Initializing JsonSerde");
		// Get the names of the columns for the table this SerDe is being used
        // with
        String columnNameProperty = tblProps
                        .getProperty(Constants.LIST_COLUMNS);
        columnNames = Arrays.asList(columnNameProperty.split(","));
        
        // Convert column types from text to TypeInfo objects
        String columnTypeProperty = tblProps
                        .getProperty(Constants.LIST_COLUMN_TYPES);
        
        columnTypes = TypeInfoUtils
                .getTypeInfosFromTypeString(columnTypeProperty);
        assert columnNames.size() == columnTypes.size();
        numColumns = columnNames.size();
        
        // Create ObjectInspectors from the type information for each column
        List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(
                        columnNames.size());
        ObjectInspector oi;
        for (int c = 0; c < numColumns; c++) {
            oi = TypeInfoUtils
                            .getStandardJavaObjectInspectorFromTypeInfo(columnTypes
                                            .get(c));
            columnOIs.add(oi);
        }
        rowOI = ObjectInspectorFactory.getStandardStructObjectInspector(
                columnNames, columnOIs);
        // Create an empty row object to be reused during deserialization
        row = new ArrayList<Object>(numColumns);
        for (int c = 0; c < numColumns; c++) {
                row.add(null);
        }

        LOG.debug("JsonSerde initialization complete");
    
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.hive.serde2.Deserializer#deserialize(org.apache.hadoop.io.Writable)
	 */
	@Override
	public Object deserialize(Writable blob) throws SerDeException {
        Text rowText = (Text) blob;
        LOG.debug("Deserialize row: " + rowText.toString());
        // Try parsing row into JSON object
        JSONObject jObj;
        try {
        	 jObj = new JSONObject(rowText.toString()) {
                 /**
                  * In Hive column names are case insensitive, so lower-case all
                  * field names
                  * 
                  * @see engineering.old.json.JSONObject#put(java.lang.String,
                  *      java.lang.Object)
                  */
                 @Override
                 public JSONObject put(String key, Object value)
                                 throws JSONException {
                         return super.put(key.toLowerCase(), value);
                 }
         };
        } 
        catch (JSONException e) {
            // If row is not a JSON object, make the whole row NULL
            LOG.error("Row is not a valid JSON Object - JSONException: "
                            + e.getMessage());
            return null;
        }
        // Loop over columns in table and set values
        String colName;
        Object value;
        for (int c = 0; c < numColumns; c++) {
        	 colName = columnNames.get(c);
             TypeInfo ti = columnTypes.get(c);
             try {
                 // Get type-safe JSON values
                 if (jObj.isNull(colName)) {
                         value = null;
                 } else if (ti.getTypeName().equalsIgnoreCase(
                                 Constants.DOUBLE_TYPE_NAME)) {
                         value = jObj.getDouble(colName);
                 } else if (ti.getTypeName().equalsIgnoreCase(
                                 Constants.BIGINT_TYPE_NAME)) {
                         value = jObj.getLong(colName);
                 } else if (ti.getTypeName().equalsIgnoreCase(
                                 Constants.INT_TYPE_NAME)) {
                         value = jObj.getInt(colName);
                 } else if (ti.getTypeName().equalsIgnoreCase(
                                 Constants.TINYINT_TYPE_NAME)) {
                         value = Byte.valueOf(jObj.getString(colName));
                 } else if (ti.getTypeName().equalsIgnoreCase(
                                 Constants.FLOAT_TYPE_NAME)) {
                         value = Float.valueOf(jObj.getString(colName));
                 } else if (ti.getTypeName().equalsIgnoreCase(
                                 Constants.BOOLEAN_TYPE_NAME)) {
                         value = jObj.getBoolean(colName);
                 } else if (ti.getTypeName().equalsIgnoreCase(
                                 Constants.STRING_TYPE_NAME)) {
                         value = jObj.getString(colName);
                 } else if (ti.getTypeName()
                		 .startsWith(Constants.LIST_TYPE_NAME)) {
                	 // Copy to an Object array
                     JSONArray jArray = jObj.getJSONArray(colName);
                     Object[] newarr = new Object[jArray.length()];
                     for (int i = 0; i < newarr.length; i++) {
                             newarr[i] = jArray.get(i);
                     }
                     value = newarr;
                 }else {
                     // Fall back, just get an object
                     value = jObj.get(colName);
                 }
             
             }
             catch (JSONException e) {
                 // If the column cannot be found, just make it a NULL value and
                 // skip over it
                 if (LOG.isDebugEnabled()) {
                         LOG.debug("Column '" + colName + "' not found in row: "
                                         + rowText.toString() + " - JSONException: "
                                         + e.getMessage());
                 }
                 value = null;
         }
         row.set(c, value);
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
		// TODO Auto-generated method stub
		return null;
	}
	
}