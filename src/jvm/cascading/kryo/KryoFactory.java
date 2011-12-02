package cascading.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.ObjectBuffer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;

/** User: sritchie Date: 12/1/11 Time: 3:18 PM */
public class KryoFactory {
    public static final Logger LOG = Logger.getLogger(KryoFactory.class);

    /**
     * Initial capacity of the Kryo object buffer, used for deserializing tuple entries.
     */
    private static final int INIT_CAPACITY = 2000;

     /**
     * Maximum capacity of the Kryo object buffer.
     */
    private static final int FINAL_CAPACITY = 2000000000;

    /**
     * KRYO_SERIALIZATIONS holds a colon-separated list of classes to register with Kryo.
     * For example:
     *
     * "someClass,someSerializer:otherClass:thirdClass,thirdSerializer"
     *
     * would register someClass and thirdClass with custom serializers and otherClass with
     * Kryo's FieldSerializer. The FieldSerializer requires the class to
     * implement a default constructor.
     */
    public static final String KRYO_SERIALIZATIONS = "cascading.kryo.serializations";

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
     * Encodes the supplied registrations HashMap as a comma-separated list of alternating keys
     * and values and stores this in the JobConf under the SERIALIZATION_PAIRS key.
     * @param conf: Hadoop JobConf.
     * @param registrations: HashMap of className -> kryoSerializerClassName || null
     */
    public void setSerializations(JobConf conf, HashMap<String, String> registrations) {
        int i = 0;
        int finalIdx = registrations.size() - 1;
        StringBuilder builder = new StringBuilder();

        for(String klassName: registrations.keySet()) {
            String serializerClassName = registrations.get(klassName);
            builder.append(klassName);

            if (serializerClassName != null)
                builder.append("," + serializerClassName);

            if (i++ != finalIdx)
                builder.append(":");
        }
        
        conf.set(KRYO_SERIALIZATIONS, builder.toString());
    }

    /**
     * Retrieves all Kryo serializations from the JobConf as a HashMap.
     * @param conf: Hadoop jobConf
     * @return HashMap of [klassName, kryoSerializationClassName] pairs
     */
    public HashMap getSerializations(JobConf conf) {
        String serializationString = conf.get(KRYO_SERIALIZATIONS);
        HashMap<String, String> builder = new HashMap<String, String>();

        if (serializationString == null) return builder;

        // Build up a HashMap of class, serializerClass string pairs.
        String key = null;
        for (String s: serializationString.split(":")) {
            String[] pair = s.split(",");
            if (pair.length == 2)
                builder.put(pair[0], pair[1]);
            else
                builder.put(pair[0], null);
        }
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
        return conf.getBoolean(ACCEPT_ALL, true);
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
