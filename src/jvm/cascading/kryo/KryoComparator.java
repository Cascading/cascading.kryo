package cascading.kryo;

import java.io.Serializable;

import cascading.tuple.StreamComparator;
import cascading.tuple.hadoop.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Comparator;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

/** User: sritchie Date: 12/11/11 Time: 5:03 PM */
public class KryoComparator implements StreamComparator<BufferedInputStream>, Comparator, Serializable {

    public int compare(BufferedInputStream lhsStream, BufferedInputStream rhsStream) {
        try {
            //TODO: how can i get rid of creating new stream objects everytime?
            DataInputStream dleft = new DataInputStream(lhsStream);
            DataInputStream dRight = new DataInputStream(rhsStream);

            int lhsLen = WritableUtils.readVInt(dleft);
            byte[] lhs = lhsStream.getBuffer();
            int lhsPos = lhsStream.getPosition();

            int rhsLen = WritableUtils.readVInt(dRight);
            byte[] rhs = rhsStream.getBuffer();
            int rhsPos = rhsStream.getPosition();

            lhsStream.skip(lhsLen);
            rhsStream.skip(rhsLen);

            return WritableComparator.compareBytes( lhs, lhsPos, lhsLen, rhs, rhsPos, rhsLen );
        } catch(IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public int compare(Object o1, Object o2) {
        if (o1 instanceof Comparable && o2 instanceof Comparable) {
            return ((Comparable) o1).compareTo(o2);
        } else {
            throw new RuntimeException("Can't compare " + o1 + " with " + o2 + ".");
        }

    }
}
