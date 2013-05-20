package cascading.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import org.apache.hadoop.io.serializer.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.ObjectInputStream;
import java.io.IOException;
import java.io.InputStream;

public class KryoDeserializer implements Deserializer<Object> {

    private Kryo kryo;
    private final KryoSerialization kryoSerialization;
    private final Class<Object> klass;

    private DataInputStream inputStream;
    private boolean fallBackToOOS;

    public KryoDeserializer(KryoSerialization kryoSerialization, Class<Object> klass) {
        this.kryoSerialization =  kryoSerialization;
        this.klass = klass;
        this.fallBackToOOS = kryoSerialization.getConf().getBoolean("cascading.kryo.java_serialization_fallback", false);
    }

    public void open(InputStream in) throws IOException {
        kryo = kryoSerialization.populatedKryo();
        if( in instanceof DataInputStream)
            inputStream = (DataInputStream) in;
        else
            inputStream = new DataInputStream( in );
    }

    public Object deserialize(Object o) throws IOException {
        byte[] bytes = new byte[inputStream.readInt()];
        inputStream.readFully( bytes );
        if(fallBackToOOS) {
          ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
          try {
            return ois.readObject();
          } catch(Exception e) {
            e.printStackTrace();
            return null;
          }
        } else {
          return kryo.readObject(new Input(bytes), klass);
        }
    }

    // TODO: Bump the kryo version, add a kryo.reset();
    public void close() throws IOException {
        try {
            if( inputStream != null )
                inputStream.close();
        } finally {
            inputStream = null;
        }
    }
}
