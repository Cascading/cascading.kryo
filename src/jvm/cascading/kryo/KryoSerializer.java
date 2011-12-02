package cascading.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.ObjectBuffer;
import org.apache.hadoop.io.serializer.Serializer;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;

/** User: sritchie Date: 12/1/11 Time: 11:57 AM */
public class KryoSerializer implements Serializer {

    private OutputStream outputStream;
    ObjectBuffer kryoBuf;

    public KryoSerializer(Kryo k) {
        this.kryoBuf =  KryoFactory.newBuffer(k);
    }

    public void open(OutputStream out) throws IOException {
        this.outputStream = out;
    }

    public void serialize(Object o) throws IOException {
        kryoBuf.writeClassAndObject(outputStream, o);
    }

    public void close() throws IOException {
        try {
            if( outputStream != null )
                outputStream.close();
        } finally {
            outputStream = null;
        }
    }
}
