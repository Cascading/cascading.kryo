package cascading.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.ObjectBuffer;
import org.apache.hadoop.io.serializer.Serializer;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/** User: sritchie Date: 12/1/11 Time: 11:57 AM */
public class KryoSerializer implements Serializer<Object> {

    private DataOutputStream outputStream;
    ObjectBuffer kryoBuf;

    public KryoSerializer(Kryo k) {
        this.kryoBuf =  KryoFactory.newBuffer(k);
    }

    public void open(OutputStream out) throws IOException {
        if( out instanceof DataOutputStream )
            this.outputStream = (DataOutputStream) out;
        else
            this.outputStream = new DataOutputStream( out );
    }

    public void serialize(Object o) throws IOException {
        // We don't need to write the class because Hadoop provides this
        byte[] bytes = kryoBuf.writeObject(o);
        outputStream.writeInt(bytes.length);
        outputStream.write(bytes);
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
