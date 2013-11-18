package cascading.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import org.apache.hadoop.io.serializer.Deserializer;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

public class UnBufferedKryoDeserializer implements Deserializer<Object> {

    private Kryo kryo;
    private final KryoSerialization kryoSerialization;
    private final Class<Object> klass;

    private DataInputStream inputStream;

    public UnBufferedKryoDeserializer(KryoSerialization kryoSerialization, Class<Object> klass) {
        this.kryoSerialization =  kryoSerialization;
        this.klass = klass;
    }

    public void open(InputStream in) throws IOException {
        kryo = kryoSerialization.populatedKryo();

        if( in instanceof DataInputStream)
            inputStream = (DataInputStream) in;
        else
            inputStream = new DataInputStream( in );
    }

    public Object deserialize(Object o) throws IOException {
        // Get size of object.
        int bytes = inputStream.readInt();
        // Now wrap the hadoop-held stream to prevent kryo from reading
        // beyond the object boundary.
        ReadLimitingInputStream wrappedInput = new ReadLimitingInputStream(inputStream, bytes);
        return kryo.readObject(new Input(wrappedInput), klass);
    }

    public void close() throws IOException {
        try {
            if( inputStream != null )
                inputStream.close();
        } finally {
            inputStream = null;
        }
    }

    static class ReadLimitingInputStream extends InputStream {
        protected final int limit;
        protected int read;
        protected final InputStream input;

        public ReadLimitingInputStream(InputStream input, int limit) {
            this.limit = limit;
            this.input = input;
            this.read = 0;
        }

        public int read() throws IOException {
            if(read++ < limit) {
                return input.read();
            } else {
                throw new IOException("tried to read beyond limit: " + limit);
            }
        }
    }
}
