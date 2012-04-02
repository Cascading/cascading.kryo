package cascading.kryo;

import com.esotericsoftware.kryo.io.Output;
import org.apache.hadoop.io.serializer.Serializer;

import java.io.IOException;
import java.io.OutputStream;

/** User: sritchie Date: 12/1/11 Time: 11:57 AM */
public class KryoSerializer implements Serializer<Object> {
    private final Kryo kryo;
    private Output output;

    public KryoSerializer(Kryo kryo) {
        this.kryo =  kryo;
    }

    public void open(OutputStream out) throws IOException {
        output = new Output(out);
    }

    public void serialize(Object o) throws IOException {
        kryo.writeObject(output, o);
    }

    public void close() throws IOException {
        try {
            if( output != null )
                output.close();
        } finally {
            output = null;
        }
    }
}
