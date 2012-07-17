package cascading.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import org.apache.hadoop.io.serializer.Deserializer;

import java.io.IOException;
import java.io.InputStream;

/** User: sritchie Date: 12/1/11 Time: 3:15 PM */
public class KryoDeserializer implements Deserializer<Object> {

    private Kryo kryo;
    private final KryoSerialization kryoSerialization;
    private final Class<Object> klass;
    private Input input;

    public KryoDeserializer(KryoSerialization kryoSerialization, Class<Object> klass) {
        this.kryoSerialization =  kryoSerialization;
        this.klass = klass;
    }

    public void open(InputStream in) throws IOException {
        kryo = kryoSerialization.populatedKryo();
        input = new Input(in);
    }

    public Object deserialize(Object o) throws IOException {
        return kryo.readObject(input, klass);
    }

    // TODO: Bump the kryo version, add a kryo.reset();
    public void close() throws IOException {
        try {
            if( input != null )
                input.close();
        } finally {
            input = null;
        }
    }
}
