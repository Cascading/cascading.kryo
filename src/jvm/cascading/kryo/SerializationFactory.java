package cascading.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;

/** User: sritchie Date: 12/8/11 Time: 5:31 PM */
public interface SerializationFactory {
    public Serializer makeSerializer(Kryo k);
}
