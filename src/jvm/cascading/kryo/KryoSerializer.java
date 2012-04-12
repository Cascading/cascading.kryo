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

    private final Kryo kryo;
    private final Output output = new Output(INIT_CAPACITY, FINAL_CAPACITY);
    private DataOutputStream outputStream;

    public KryoSerializer(Kryo kryo) {
        this.kryo =  kryo;
    }

    public void open(OutputStream out) throws IOException {
        if( out instanceof DataOutputStream )
            this.outputStream = (DataOutputStream) out;
        else
            this.outputStream = new DataOutputStream(out);
    }

    public void serialize(Object o) throws IOException {
        output.clear();
        kryo.writeObject(output, o);
        byte[] bytes = output.toBytes();

        outputStream.writeInt(bytes.length);
        outputStream.write(bytes);
    }

    public void close() throws IOException {
        output.close();

        try {
            if( outputStream != null )
                outputStream.close();
        } finally {
            outputStream = null;
        }
    }
}
