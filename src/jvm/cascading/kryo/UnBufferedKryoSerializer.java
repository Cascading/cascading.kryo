package cascading.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import org.apache.hadoop.io.serializer.Serializer;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class UnBufferedKryoSerializer implements Serializer<Object> {
    private Kryo kryo;
    private final KryoSerialization kryoSerialization;
    private DataOutputStream outputStream;

    public UnBufferedKryoSerializer(KryoSerialization kryoSerialization) {
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
        // Serialize once just to find the size of the object.
        SizeCountingOutputStream scos = new SizeCountingOutputStream();
        Output sco = new Output(scos);
        kryo.writeObject(sco, o);
        sco.flush();
        int serializedSize = scos.bytesWritten();
        // Now write the size and then object directly to the buffer held by hadoop.
        outputStream.writeInt(serializedSize);
        Output ko = new Output(outputStream);
        kryo.writeObject(ko, o);
        ko.flush();
        outputStream.flush();
    }

    // TODO: Bump the kryo version, add a kryo.reset();
    public void close() throws IOException {
        try {
            if(outputStream != null) {
                outputStream.close();
            }
        } finally {
            outputStream = null;
            kryo = null;
        }
    }

    static class SizeCountingOutputStream extends OutputStream {
        protected int bytesWritten = 0;

        public int bytesWritten() {
            return bytesWritten;
        }

        public void write(int i) {
            bytesWritten++;
        }
    }
}
