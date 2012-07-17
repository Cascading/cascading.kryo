package cascading.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import org.apache.hadoop.io.serializer.Serializer;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/** User: sritchie Date: 12/1/11 Time: 11:57 AM */
public class KryoSerializer implements Serializer<Object> {

    /**
     * Initial capacity of the Kryo Output object.
     */
    private static final int INIT_CAPACITY = 2000;

    /**
     * Maximum capacity of the Kryo Output object.
     */
    private static final int FINAL_CAPACITY = 2000000000;

    private Kryo kryo;
    private final KryoSerialization kryoSerialization;
    private final Output output = new Output(INIT_CAPACITY, FINAL_CAPACITY);
    private DataOutputStream outputStream;

    public KryoSerializer(KryoSerialization kryoSerialization) {
        this.kryoSerialization =  kryoSerialization;
    }

    public void open(OutputStream out) throws IOException {
        kryo = kryoSerialization.populatedKryo();

        if( out instanceof DataOutputStream )
            outputStream = (DataOutputStream) out;
        else
            outputStream = new DataOutputStream(out);
    }

    public void serialize(Object o) throws IOException {
        output.clear();
        kryo.writeObject(output, o);
        byte[] bytes = output.toBytes();

        outputStream.writeInt(bytes.length);
        outputStream.write(bytes);
    }

    // TODO: Bump the kryo version, add a kryo.reset();
    public void close() throws IOException {
        kryo = null;
        output.close();

        try {
            if( outputStream != null )
                outputStream.close();
        } finally {
            outputStream = null;
        }
    }
}
