package cascading.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import org.apache.hadoop.io.serializer.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class KryoSerializer implements Serializer<Object> {
    private Kryo kryo;
    private final KryoSerialization kryoSerialization;
    private DataOutputStream outputStream;

    public KryoSerializer(KryoSerialization kryoSerialization) {
        this.kryoSerialization = kryoSerialization;
    }

    public void open(OutputStream out) throws IOException {
        kryo = kryoSerialization.populatedKryo();

        if(out instanceof DataOutputStream)
            outputStream = (DataOutputStream)out;
        else
            outputStream = new DataOutputStream(out);
    }

    public void serialize(Object o) throws IOException {
        // Write output to temorary buffer.
        ByteArrayOutputStream bho = new ByteArrayOutputStream();
        Output ko = new Output(bho);
        kryo.writeObject(ko, o);
        ko.flush();
        // Copy from buffer to output stream.
        outputStream.writeInt(bho.size());
        outputStream.write(bho.toByteArray(), 0, bho.size());
        outputStream.flush();
    }

    // TODO: Bump the kryo version, add a kryo.reset();
    public void close() throws IOException {
        try {
            if(outputStream != null)
                outputStream.close();
        } finally {
            outputStream = null;
            kryo = null;
        }
    }
}
