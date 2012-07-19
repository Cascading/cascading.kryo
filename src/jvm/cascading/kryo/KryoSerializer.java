package cascading.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import org.apache.hadoop.io.serializer.Serializer;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class KryoSerializer implements Serializer<Object> {
    private Kryo kryo;
    private final KryoSerialization kryoSerialization;
    private final Output output = new Output(
        KryoSerialization.OUTPUT_BUFFER_SIZE, KryoSerialization.MAX_OUTPUT_BUFFER_SIZE);
    private DataOutputStream outputStream;
    private int prevPosition;

    public KryoSerializer(KryoSerialization kryoSerialization) {
        this.kryoSerialization =  kryoSerialization;
    }

    public void open(OutputStream out) throws IOException {
        kryo = kryoSerialization.populatedKryo();

        if( out instanceof DataOutputStream)
            outputStream = (DataOutputStream) out;
        else
            outputStream = new DataOutputStream(out);
    }

    // We tidy prevent output from maintaining a giant internal buffer.
    public void tidyBuffer() {
        int currentPosition = output.position();

        // If the previous serialized object was large (greater than the switch size) and the current
        // object falls below the switch size, reset the buffer to be small again. If both objects
        // are small (or large), no reallocation occurs.
        if (prevPosition > KryoSerialization.SWITCH_LIMIT && currentPosition <= KryoSerialization.SWITCH_LIMIT)
            output.setBuffer(new byte[KryoSerialization.OUTPUT_BUFFER_SIZE], KryoSerialization.MAX_OUTPUT_BUFFER_SIZE);

        prevPosition = currentPosition;
        output.clear();
    }

    public void serialize(Object o) throws IOException {
        kryo.writeObject(output, o);
        byte[] bytes = output.toBytes();

        outputStream.writeInt(bytes.length);
        outputStream.write(bytes);

        tidyBuffer();
    }

    // TODO: Bump the kryo version, add a kryo.reset();
    public void close() throws IOException {
        kryo = null;

        try {
            if( outputStream != null ) {
                outputStream.close();
            }
        } finally {
            outputStream = null;
        }
    }
}
