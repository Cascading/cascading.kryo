package cascading.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import org.apache.hadoop.io.serializer.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class KryoSerializer implements Serializer<Object> {
  private DataOutputStream os;
  private KryoSerialization ks;
  private Kryo kryo;

  public KryoSerializer(KryoSerialization kryoSerialization) {
    ks = kryoSerialization;
  }

  public void open(OutputStream out) throws IOException {
    if(out instanceof DataOutputStream)
      os = (java.io.DataOutputStream)out;
    else
      os = new DataOutputStream(out);
    kryo = ks.populatedKryo();
  }

  public void serialize(Object o) throws IOException {
    // Write output to temorary buffer.
    ByteArrayOutputStream bho = new ByteArrayOutputStream();
    Output ko = new Output(bho);
    kryo.writeObject(ko, o);
    ko.flush();
    // Copy from buffer to output stream.
    os.writeInt(bho.size());
    os.write(bho.toByteArray(), 0, bho.size());
    os.flush();
  }

  // TODO: Bump the kryo version, add a kryo.reset();
  public void close() throws IOException {
    try {
      if(os != null)
        os.close();
    } catch(Exception e) {
      e.printStackTrace();
    } finally {
      os = null;
      kryo = null;
    }
  }
}
