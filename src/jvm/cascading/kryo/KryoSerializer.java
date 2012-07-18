package cascading.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import org.apache.hadoop.io.serializer.Serializer;

import java.io.IOException;
import java.io.OutputStream;

/** User: sritchie Date: 12/1/11 Time: 11:57 AM */
@SuppressWarnings("FieldCanBeLocal")
public class KryoSerializer implements Serializer<Object> {
    private final int OUTPUT_BUFFER_SIZE = 1<<12;
    private final int MAX_OUTPUT_BUFFER_SIZE = 1<<24;

    private Kryo kryo;
    private final KryoSerialization kryoSerialization;
    private Output output;

    public KryoSerializer(KryoSerialization kryoSerialization) {
        this.kryoSerialization =  kryoSerialization;
    }

    public void open(OutputStream out) throws IOException {
        kryo = kryoSerialization.populatedKryo();
        output = new Output(OUTPUT_BUFFER_SIZE, MAX_OUTPUT_BUFFER_SIZE);
        output.setOutputStream(out);
    }

    public void serialize(Object o) throws IOException {
        kryo.writeObject(output, o);
        output.flush();
    }

    // TODO: Bump the kryo version, add a kryo.reset();
    public void close() throws IOException {
        kryo = null;

        try {
            if( output != null ) {
                output.close();
            }
        } finally {
            output = null;
        }
    }
}
