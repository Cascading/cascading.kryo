package cascading.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import org.apache.hadoop.io.serializer.Deserializer;

import java.io.IOException;
import java.io.InputStream;

/** User: sritchie Date: 12/1/11 Time: 3:15 PM */
public class KryoDeserializer implements Deserializer<Object> {

    private Kryo kryo;
    private final KryoSerialization kryoSerialization;
    private final Class<Object> klass;
    private Input input;

    public KryoDeserializer(KryoSerialization kryoSerialization, Class<Object> klass) {
        this.kryoSerialization =  kryoSerialization;
        this.klass = klass;
    }

    public void open(InputStream in) throws IOException {
        if (!in.markSupported())
            throw new IOException("InputStream must support `mark`.");

        kryo = kryoSerialization.populatedKryo();
        input = new Input(in);
    }

    // As of version 2.16, Kryo overfills its internal buffer by slurping with wild
    // abandon from its wrapped input stream. The following method compensates for this
    // by resetting Input's position and limit after each slurp and moving the wrapped inputstream's
    // pointer forward manually.

    public Object deserialize(Object o) throws IOException {
        // Marking phase.
        InputStream inStream = input.getInputStream();
        inStream.mark(KryoSerialization.MAX_OUTPUT_BUFFER_SIZE);
        int startingPos = input.position();

        // Reading phase.
        Object obj = kryo.readObject(input, klass);
        int finalPos = input.position();

        // Skipping phase.
        // inStream resets to the observed mark above. Resetting input's limit and position forces
        // input to continue reading from its wrapped stream at the marked position.
        inStream.reset();
        input.setPosition(0);
        input.setLimit(0);
        int needToSkip = finalPos - startingPos;

        while(needToSkip > 0)
            needToSkip -= inStream.skip(needToSkip);

        return obj;
    }

    // TODO: Bump the kryo version, add a kryo.reset();
    public void close() throws IOException {
        try {
            if( input != null )
                input.close();
        } finally {
            input = null;
        }
    }
}
