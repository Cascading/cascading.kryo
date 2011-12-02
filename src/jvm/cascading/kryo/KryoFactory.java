package cascading.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.ObjectBuffer;
import com.esotericsoftware.kryo.serialize.MapSerializer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import java.util.HashMap;

/** User: sritchie Date: 12/1/11 Time: 3:18 PM */
public class KryoFactory {
    public static final Logger LOG = Logger.getLogger(KryoSerializer.class);
    private static final int INIT_CAPACITY = 2000;
    private static final int FINAL_CAPACITY = 2000000000;

    public static final String KRYO_CONF = "cascading.kryo.serializations";
    public static final String KRYO_REG_OPTIONAL = "cascading.kryo.acceptall";

    private ObjectBuffer buf;

    public KryoFactory() {
        buf = bareBuffer();
    }

    private ObjectBuffer bareBuffer() {
        Kryo kryo = new Kryo();
        kryo.register(HashMap.class, new MapSerializer(kryo));
        return new ObjectBuffer(kryo);
    }

    // I think this should actually just read a string of alternating pairs;
    // class -> kryoserialization. This is NOT the way to go, as it doesn't
    // allow the user to specify registration pairs without going through these
    // particular methods.
    public void setRegistrations(JobConf conf, HashMap<String, String> regMap) {
        conf.set(KRYO_CONF, StringUtils.byteToHexString(buf.writeObject(regMap)));
    }

    public HashMap getRegistrations(JobConf conf) {
        String s = conf.get(KRYO_CONF);
        if (s == null) return new HashMap();
        byte[] val = StringUtils.hexStringToByte(s);
        HashMap out = buf.readObject(val, HashMap.class);
        return (out == null) ? new HashMap() : out;
    }

    public void setRegistrationOptional(JobConf conf, boolean optional) {
        conf.setBoolean(KRYO_REG_OPTIONAL, optional);
    }

    public boolean getRegistrationOptional(JobConf conf) {
        return conf.getBoolean(KRYO_REG_OPTIONAL, false);
    }

    public static ObjectBuffer newBuffer(Kryo k) {
        return new ObjectBuffer(k, INIT_CAPACITY, FINAL_CAPACITY);
    }

    public static void populateKryo(Kryo k, HashMap<String, String> registrations, boolean skipMissing) {
        k.setRegistrationOptional(skipMissing);

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
