package cascading.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.ObjectBuffer;
import com.esotericsoftware.kryo.Serializer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

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
     * Encodes the supplied registrations list as a comma-separated list of alternating keys
     * and values and stores this in the JobConf under the SERIALIZATION_PAIRS key.
     * @param conf: Hadoop JobConf.
     * @param registrations: List of List of [className, kryoSerializerClassName || null]
     */
    public void setSerializations(JobConf conf, List<List<String>> registrations) {
        StringBuilder builder = new StringBuilder();

        for (Iterator<List<String>> pairIter = registrations.iterator(); pairIter.hasNext(); ) {
            List<String> registrationPair = pairIter.next();

            int size = registrationPair.size();
            if (size < 1 || size > 2) {
                throw new RuntimeException(registrationPair + " must contain either 1 or 2 entries.");
            }

            for (Iterator<String> it = registrationPair.iterator(); it.hasNext(); ) {
                builder.append(it.next());
                if (it.hasNext()) {
                    builder.append(",");
                }
            }
            if (pairIter.hasNext())
                builder.append(":");
        }

        conf.set(KRYO_SERIALIZATIONS, builder.toString());
    }

    /**
     * Retrieves all Kryo serializations from the JobConf as a HashMap.
     * @param conf: Hadoop jobConf
     * @return HashMap of [klassName, kryoSerializationClassName] pairs
     */
    public List<List<String>> getSerializations(JobConf conf) {
        String serializationString = conf.get(KRYO_SERIALIZATIONS);
        List<List<String>> builder = new ArrayList<List<String>>();

        if (serializationString == null) return builder;

        // Build up a List of class, serializerClass string pairs.
        String key = null;
        for (String s: serializationString.split(":")) {
            String[] pair = s.split(",");
            if (pair.length == 2)
                builder.add(Arrays.asList(pair[0], pair[1]));
            else
                builder.add(Arrays.asList(pair[0]));
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
        return newBuffer(k, INIT_CAPACITY);
    }

    public static ObjectBuffer newBuffer(Kryo k, int initCapacity) {
        return newBuffer(k, initCapacity, FINAL_CAPACITY);
    }

    public static ObjectBuffer newBuffer(Kryo k, int initCapacity, int finalCapacity) {
        return new ObjectBuffer(k, initCapacity, finalCapacity);
    }

    public static void populateKryo(Kryo k, List<List<String>> registrations,
        boolean skipMissing, boolean acceptAll) {
        k.setRegistrationOptional(acceptAll);

        for (List<String> registrationPair: registrations) {
            String className;
            String serializerClassName = null;

            int size = registrationPair.size();
            if (size == 1)
                className = registrationPair.get(0);
            else if (size == 2) {
                className = registrationPair.get(0);
                serializerClassName = registrationPair.get(1);
            } else {
                throw new RuntimeException("Too many entries in the Kryo registrations map!");
            }

            try {
                Class klass = Class.forName(className);
                Class serializerClass = null;

                if (serializerClassName != null) {
                    serializerClass = Class.forName(serializerClassName);
                }

                if (serializerClass == null) {
                    k.register(klass);
                } else {
                    k.register(klass, (Serializer) serializerClass.newInstance());
                }

            } catch (ClassNotFoundException e) {
                if (skipMissing) {
                    LOG.info("Could not find serialization or class for " + serializerClassName
                             + ". Skipping registration.");
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
