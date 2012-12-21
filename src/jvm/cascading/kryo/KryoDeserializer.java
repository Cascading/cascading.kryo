package cascading.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import org.apache.hadoop.io.serializer.Deserializer;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

public class KryoDeserializer implements Deserializer<Object> {

  private java.io.DataInputStream is;
  private Class<Object> klass;
  private KryoSerialization ks;
  private Kryo kryo;

  public KryoDeserializer(KryoSerialization kryoSerialization, Class<Object> klass) {
    this.klass = klass;
    ks = kryoSerialization;
  }

  public void open(InputStream in) throws IOException {
    is = (DataInputStream)in;
    kryo = ks.populatedKryo();
  }

  public Object deserialize(Object o) throws IOException {
    int sz = is.readInt();
    byte[] b  = new byte[sz];
    is.read(b);
    //return(new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(b))).readObject();
    return(kryo.readObject(new Input(b), klass));
  }

  // TODO: Bump the kryo version, add a kryo.reset();
  public void close() throws IOException {
    try {
      if(is != null) is.close();
    } catch(Exception e ) {
      e.printStackTrace();
    } finally {
      is = null;
      kryo = null;
    }
  }
}
