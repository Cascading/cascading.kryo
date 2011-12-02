package cascading.kryo;

import com.esotericsoftware.kryo.Kryo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import java.util.HashMap;

/** User: sritchie Date: 12/1/11 Time: 11:43 AM */
public class KryoSerialization extends Configured implements Serialization {
    public static final Logger LOG = Logger.getLogger(KryoSerialization.class);

    HashMap registrations;
    boolean skipMissing, acceptAll;
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

    /**
     * Instantiating Kryo in a separate method makes it easy for subclasses to pre-populate
     * the base kryo instance. Cascalog uses this to register serializers for all clojure data
     * structures.
     * @return Base kryo instance for later customization.
     */
    public Kryo makeKryo() {
        return new Kryo();
    }

    /**
     * @return New Kryo instance populated from JobConf settings.
     */
    public Kryo populatedKryo() {
        Kryo k = makeKryo();
        KryoFactory.populateKryo(k, registrations, skipMissing, acceptAll);
        return k;
    }

    @Override public Configuration getConf() {
        if( super.getConf() == null )
            setConf( new JobConf() );
        return super.getConf();
    }

    /**
     * If Initializes Kryo instance from the JobConf on the first run. If the ACCEPT_ALL key in
     * the JobConf has been set to true, Kryo will return yes for everything; else, Kryo will only
     * return true for classes with explicitly registered serializations.
     * @param aClass
     * @return
     */
    public boolean accept(Class aClass) {
        if (kryo == null) {
            JobConf conf = (JobConf) getConf();
            KryoFactory factory = new KryoFactory();
            registrations = factory.getSerializations(conf);
            skipMissing = factory.getSkipMissing(conf);
            acceptAll = factory.getAcceptAll(conf);
            kryo = populatedKryo();
        }
        try {
            return (kryo.getRegisteredClass(aClass) != null);
        } catch (IllegalArgumentException e) {
            return acceptAll;
        }
    }

    public Serializer getSerializer(Class aClass) {
        return new KryoSerializer(populatedKryo());
    }

    public Deserializer getDeserializer(Class aClass) {
        return new KryoDeserializer(populatedKryo());
    }
}
