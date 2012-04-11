package cascading.kryo;

import com.esotericsoftware.kryo.Serializer;
import de.javakaffee.kryoserializers.KryoReflectionFactorySupport;

/** User: sritchie Date: 2/9/12 Time: 6:16 PM */
public class Kryo extends KryoReflectionFactorySupport {
    public void registerHierarchy(Class superClass, Serializer serializer) {
        addDefaultSerializer(superClass, serializer);
    }
}
