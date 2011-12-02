package cascading.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.ObjectBuffer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;

/** User: sritchie Date: 12/1/11 Time: 3:18 PM */
public class KryoFactory {
    public static final Logger LOG = Logger.getLogger(KryoSerializer.class);

    /**
     * Initial capacity of the Kryo object buffer, used for deserializing tuple entries.
     */
    private static final int INIT_CAPACITY = 2000;

     /**
     * Maximum capacity of the Kryo object buffer.
     */
    private static final int FINAL_CAPACITY = 2000000000;

    /**
     * FIELDS_SERIALIZATIONS holds a comma-separated list of classes to register with Kryo
     * with no custom serializater. Usually this causes Kryo to default to its FieldsSerializer,
     * which is similar to java Serialization.
     */
    public static final String FIELDS_SERIALIZATIONS = "cascading.kryo.fields.serializations";

    /**
     * SERIALIZATION_PAIRS holds a comma-separated list of alternating
     * klassName & kryoSerializationClassName. Use this key to specify custom serializaters for
     * various classes.
     */
    public static final String SERIALIZATION_PAIRS = "cascading.kryo.serialization.pairs";


    /**
     * If SKIP_MISSING is set to true, Kryo won't throw an error when Cascading tries to register
     * a class or serialization that doesn't exist.
     */
    public static final String SKIP_MISSING = "cascading.kryo.skip.missing";

    /**
     * If ACCEPT_ALL is set to true, Kryo will try to serialize all java objects, not just those
     * with custom serializations registered.
     */
    public static final String ACCEPT_ALL = "cascading.kryo.accept.all";


    /**
     * Converts the supplied sequence of registrations into a comma-separated list and stores
     * it in the supplied JobConf under FIELDS_SERIALIZATIONS.
     *
     * @param conf: Hadoop JobConf.
     * @param registrations: an ArrayList of class names to register with Kryo.
     */
    public void setFieldsSerializations(JobConf conf, ArrayList<String> registrations) {
        int i = 0;
        int finalIdx = registrations.size() - 1;

        StringBuilder builder = new StringBuilder();
        for (String klassName: registrations) {
            builder.append(klassName);
            if (i++ != finalIdx)
                builder.append(",");
        }
        conf.set(FIELDS_SERIALIZATIONS, builder.toString());
    }

    /**
     * Encodes the supplied registrations HashMap as a comma-separated list of alternating keys
     * and values and stores this in the JobConf under the SERIALIZATION_PAIRS key.
     * @param conf: Hadoop JobConf.
     * @param registrations: HashMap of className -> kryoSerializerClassName
     */
    public void setSerializationPairs(JobConf conf, HashMap<String, String> registrations) {
        int i = 0;
        int finalIdx = registrations.size() - 1;
        StringBuilder builder = new StringBuilder();
        for(String klassName: registrations.keySet()) {
            String serializerClassName = registrations.get(klassName);
            builder.append(klassName + "," + serializerClassName);
            if (i++ != finalIdx)
                builder.append(",");
        }
        
        conf.set(SERIALIZATION_PAIRS, builder.toString());
    }

    /**
     * Retrieves all Kryo serializations from the JobConf as a HashMap. Serialization pairs
     * are added as proper key-value pairs, while the Fields serializations all use null values.
     * @param conf: Hadoop jobConf
     * @return HashMap of [klassName, kryoSerializationClassName] pairs
     */
    public HashMap getSerializations(JobConf conf) {
        String pairString = conf.get(SERIALIZATION_PAIRS, "");
        String fieldString = conf.get(FIELDS_SERIALIZATIONS, "");
        HashMap<String, String> builder = new HashMap<String, String>();

        // Build up a HashMap of class, serializerClass string pairs.
        String key = null;
        for (String s: pairString.split(",")) {
            if (key == null)
                key = s;
            else {
                builder.put(key, s);
                key = null;
            }
        }

        // Add all classes with no specific serializations
        for (String s: fieldString.split(","))
            builder.put(s, null);

        return builder;
    }

    public void setSkipMissing(JobConf conf, boolean optional) {
        conf.setBoolean(SKIP_MISSING, optional);
    }

    public boolean getSkipMissing(JobConf conf) {
        return conf.getBoolean(SKIP_MISSING, false);
    }

    public void setAcceptAll(JobConf conf, boolean acceptAll) {
        conf.setBoolean(ACCEPT_ALL, acceptAll);
    }

    public boolean getAcceptAll(JobConf conf) {
        return conf.getBoolean(ACCEPT_ALL, false);
    }

    public static ObjectBuffer newBuffer(Kryo k) {
        return new ObjectBuffer(k, INIT_CAPACITY, FINAL_CAPACITY);
    }

    public static void populateKryo(
        Kryo k, HashMap<String, String> registrations, boolean skipMissing, boolean acceptAll) {
        k.setRegistrationOptional(acceptAll);

        for(String klassName: registrations.keySet()) {
            String serializerClassName = registrations.get(klassName);
            try {
                Class klass = Class.forName(klassName);
                Class serializerClass = null;
                if(serializerClassName!=null)
                    serializerClass = Class.forName(serializerClassName);
                if(serializerClass == null) {
                    k.register(klass);
                } else {
                    k.register(klass, (com.esotericsoftware.kryo.Serializer) serializerClass.newInstance());
                }

            } catch (ClassNotFoundException e) {
                if(skipMissing) {
                    LOG.info("Could not find serialization or class for " + serializerClassName + ". Skipping registration.");
                } else {
                    throw new RuntimeException(e);
                }
            } catch (InstantiationException e) {
                throw new RuntimeException(e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
