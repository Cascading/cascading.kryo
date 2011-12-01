package cascading.kryo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.JobConf;

/** User: sritchie Date: 12/1/11 Time: 11:43 AM
 *
 * By extending Configured, we get access to  
 * */
public class KryoSerialization extends Configured implements Serialization {

    public static final String KRYO_CONF = "cascading.kryo.serializations";

    public KryoSerialization()
    {
    }

    /**
     * Constructor KryoSerialization creates a new KryoSerialization instance.
     *
     * @param conf of type Configuration
     */
    public KryoSerialization( Configuration conf )
    {
        super( conf );
    }

    @Override public Configuration getConf()
    {
        if( super.getConf() == null )
            setConf( new JobConf() );

        return super.getConf();
    }

    public boolean accept(Class aClass) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Serializer getSerializer(Class aClass) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Deserializer getDeserializer(Class aClass) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
