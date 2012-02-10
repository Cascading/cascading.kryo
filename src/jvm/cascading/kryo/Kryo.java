package cascading.kryo;

import com.esotericsoftware.kryo.Serializer;
import de.javakaffee.kryoserializers.KryoReflectionFactorySupport;

import java.util.ArrayList;
import java.util.List;

/** User: sritchie Date: 2/9/12 Time: 6:16 PM */
public class Kryo extends KryoReflectionFactorySupport {

    List<HierarchyPair> hierarchyPairs = new ArrayList<HierarchyPair>();

    public class HierarchyPair {
        final Class superClass;
        final Serializer serializer;
        
        public HierarchyPair(Class superClass, Serializer serializer) {
            this.superClass = superClass;
            this.serializer = serializer;
        }
        
        public Serializer retrieveSerializer(Class subClass) {
            if (superClass.isAssignableFrom(subClass))
                return serializer;
            return null;
        }
    }

    public void registerHierarchy(Class superClass, Serializer serializer) {
        hierarchyPairs.add(new HierarchyPair(superClass, serializer));
    }
    
    @Override public Serializer newSerializer(Class type) {
        for(HierarchyPair pair: hierarchyPairs) {
            Serializer ser = pair.retrieveSerializer(type);
            if(ser != null) {
                return ser;
            }
        }

        return super.newSerializer(type);
    }
}
