package openkb.hive.udtf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class DoubleColumn extends GenericUDTF {

	private PrimitiveObjectInspector targetColumnOI = null;
	private PrimitiveObjectInspector extraColumn = null;
	private List<PrimitiveObjectInspector> extraColumnList; 
	private List<Object> outputColumnAddList = new ArrayList<Object>();
	private static int DEFAULT_RET_COLUMN = 2;

	@Override
	public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
                extraColumnList = new ArrayList<PrimitiveObjectInspector>();
		ArrayList<String> fieldNames = new ArrayList<String>();
		ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

		fieldNames.add("col_first");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

		fieldNames.add("col_second");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

		targetColumnOI = (PrimitiveObjectInspector) args[0];
		if (targetColumnOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
			throw new UDFArgumentException("The target column isn't string.");
		}

		int inputColumnNum = args.length;
		if (inputColumnNum > 1) {
			for (int i=1; i < inputColumnNum; i++) {
				extraColumn = (PrimitiveObjectInspector) args[i];
				extraColumnList.add(extraColumn);
				fieldNames.add("col_extra" + i);
				fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
			}
		}
		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
	}

	@Override
	public void process(Object[] args) throws HiveException {

		outputColumnAddList = new ArrayList<Object>();

		String targetColumn = (String) targetColumnOI.getPrimitiveJavaObject(args[0]);
		if (StringUtils.isBlank(targetColumn)) {
			return;
		}

		for (int i=1; i < args.length; i++) {
			outputColumnAddList.add(extraColumnList.get(i-1).getPrimitiveJavaObject(args[i]));
		}

                int objectSize = DEFAULT_RET_COLUMN;
                if (extraColumnList != null && !extraColumnList.isEmpty()) {
                  objectSize += extraColumnList.size();
                }
                Object[] ret = new Object[objectSize];
                ret[0] = targetColumn;
                ret[1] = targetColumn;
                if (outputColumnAddList != null && !outputColumnAddList.isEmpty()) { 
                  for (int i = 0; i < outputColumnAddList.size(); i++) {
                    ret[DEFAULT_RET_COLUMN +i] = outputColumnAddList.get(i);
               }
            }

            forward(ret);

	}

	@Override
	public void close() throws HiveException {
	}

	@Override
	public String toString() {
		return "double_column";
	}

}
