package ru.fj.customudf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

public class MyUdf extends GenericUDF {

    private static final int UA_TYPE = 0;
    private static final int UA_FAMILY = 1;
    private static final int OS_NAME = 2;
    private static final int DEVICE = 3;

    private StringObjectInspector objectInspector;
    private UserAgentParser parser = new UserAgentParser();

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length > 1) {
            throw new UDFArgumentLengthException("only takes one argument");
        }
        ObjectInspector oInspector = arguments[0];
        if (!(oInspector instanceof StringObjectInspector)) {
            throw new UDFArgumentException("first argument must be string" + oInspector.getTypeName());
        }
        objectInspector = (StringObjectInspector) oInspector;
        return ObjectInspectorFactory
                .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        List<String> result = new ArrayList<String>();
        result.addAll(Arrays.asList(UserAgentParser.UNKNOWN, UserAgentParser.UNKNOWN, UserAgentParser.UNKNOWN,
                UserAgentParser.UNKNOWN));
        String inputString = objectInspector.getPrimitiveJavaObject(arguments[0].get());
        if (inputString == null) {
            return null;
        }
        UserAgentDTO userAgent = parser.parse(inputString);
        result.set(UA_TYPE, userAgent.type);
        result.set(UA_FAMILY, userAgent.family);
        result.set(OS_NAME, userAgent.osName);
        result.set(DEVICE, userAgent.device);
        return result.toArray();
    }

    @Override
    public String getDisplayString(String[] children) {
        return "MyUdf";
    }

}
