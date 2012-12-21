package cascading.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import org.apache.hadoop.io.serializer.Serializer;

import java.io.IOException;
import java.io.OutputStream;

public class KryoSerializer implements Serializer<Object> {
    private java.io.DataOutputStream os;
    private KryoSerialization ks;
    private Kryo kryo;

    public KryoSerializer(KryoSerialization kryoSerialization) {
      ks = kryoSerialization;
    }

    public void open(OutputStream out) throws IOException {
        os = (java.io.DataOutputStream)out;
        kryo = ks.populatedKryo();
    }

    public void serialize(Object o) throws IOException {
      System.out.println("about to serialize a " + o.getClass().getName());
      // get siez in bytes of serialized form.
      FakeOutputStream fo = new FakeOutputStream();
      //(new java.io.ObjectOutputStream(fo)).writeObject(o);
      Output ko = new Output(fo);
      kryo.writeObject(ko, o);
      ko.flush();
      int size = fo.size;
      System.out.println("size " + size + " bytes");
      java.io.ByteArrayOutputStream bho = new java.io.ByteArrayOutputStream(size);
      //(new java.io.ObjectOutputStream(bho)).writeObject(o);
      ko = new Output(bho);
      kryo.writeObject(ko, o);
      ko.flush();
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

    static class FakeOutputStream extends java.io.OutputStream {
      int size = 0;
      public void write(int b) {
        size += 4;
      }
      public void write(byte[] b) {
        size += b.length;
      }
      public void write(byte[] b, int off, int len) {
        size += len;
      }
    }
}
