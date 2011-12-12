package cascading.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.ObjectBuffer;
import org.apache.hadoop.io.serializer.Deserializer;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

/** User: sritchie Date: 12/1/11 Time: 3:15 PM */
public class KryoDeserializer implements Deserializer {

    private DataInputStream inputStream;
    ObjectBuffer kryoBuf;

    public KryoDeserializer(Kryo k) {
        this.kryoBuf =  KryoFactory.newBuffer(k);
    }

    public void open(InputStream in) throws IOException {
        if( in instanceof DataInputStream)
            this.inputStream = (DataInputStream) in;
        else
            this.inputStream = new DataInputStream( in );
    }

    public Object deserialize(Object o) throws IOException {
        int len = inputStream.readInt();
        byte[] bytes = new byte[len];
        inputStream.readFully( bytes );

        return kryoBuf.readClassAndObject(bytes);
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
