import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Pair implements WritableComparable<Pair> {

    private Text first;
    private Text second;

    public Pair() {
    }

    public Pair(Text left, Text right) {
    	

        this.first = left;
        this.second = right;
    }

    public void readFields(DataInput in) throws IOException {
        first = new Text(in.readUTF());
        second = new Text(in.readUTF());
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(first.toString());
        out.writeUTF(second.toString());
    }

    public void set(Text prev, Text cur) {
        first = prev;
        second = cur;
    }

    @Override
    public String toString() {
        return first.toString() + ", " + second.toString();
    }

    @Override
    public int hashCode() {
        return first.hashCode() + second.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Pair) {
            Pair pair = (Pair) o;
            return (first.equals(pair.first) || second.equals(pair.first)) 
            		&& (first.equals(pair.second) || second.equals(pair.second));
        }
        return false;
    }

    public int compareTo(Pair tp) {
        int cmp = first.compareTo(tp.first);
        if (cmp != 0) {
            return cmp;
        }
        return second.compareTo(tp.second);
    }

}

