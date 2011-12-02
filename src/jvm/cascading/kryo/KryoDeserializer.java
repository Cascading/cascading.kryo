package cascading.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.ObjectBuffer;
import org.apache.hadoop.io.serializer.Deserializer;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;

/** User: sritchie Date: 12/1/11 Time: 3:15 PM */
public class KryoDeserializer implements Deserializer {

    private InputStream inputStream;
    ObjectBuffer kryoBuf;

    public KryoDeserializer(Kryo k) {
        this.kryoBuf =  KryoFactory.newBuffer(k);
    }

    public void open(InputStream in) throws IOException {
        this.inputStream = in;
    }

    public Object deserialize(Object o) throws IOException {
        return kryoBuf.readClassAndObject(inputStream);
    }

    public void close() throws IOException {
        try {
            if( inputStream != null )
                inputStream.close();
        } finally {
            inputStream = null;
        }
    }
}
