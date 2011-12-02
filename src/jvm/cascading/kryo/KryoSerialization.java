package cascading.kryo;

import com.esotericsoftware.kryo.Kryo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.JobConf;

import java.util.HashMap;

/** User: sritchie Date: 12/1/11 Time: 11:43 AM
 *
 * By extending Configured, we get access to  
 * */
public class KryoSerialization extends Configured implements Serialization {
    HashMap registrations;
    boolean optional;
    Kryo kryo;

    public KryoSerialization() {
    }

    /**
     * Constructor KryoSerialization creates a new KryoSerialization instance.
     *
     * @param conf of type Configuration
     */
    public KryoSerialization( Configuration conf ) {
        super( conf );
    }

    public Kryo makeKryo() {
        return new Kryo();
    }

    public Kryo populatedKryo() {
        Kryo k = makeKryo();
        KryoFactory.populateKryo(k, registrations, optional);
        return k;
    }

    @Override public Configuration getConf() {
        if( super.getConf() == null )
            setConf( new JobConf() );
        return super.getConf();
    }

    public boolean accept(Class aClass) {
        if (kryo == null) {
            JobConf conf = (JobConf) getConf();
            KryoFactory factory = new KryoFactory();
            registrations = factory.getRegistrations(conf);
            optional = factory.getRegistrationOptional(conf);
            kryo = populatedKryo();
        }
        try {
            return (kryo.getRegisteredClass(aClass) != null);
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    public Serializer getSerializer(Class aClass) {
        return new KryoSerializer(populatedKryo());
    }

    public Deserializer getDeserializer(Class aClass) {
        return new KryoDeserializer(populatedKryo());
    }
}
