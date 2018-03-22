import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by colntrev on 2/18/18.
 */
public class EdgePair implements WritableComparable<EdgePair>{
    private int[] edge;
    public EdgePair(){
        super();
        edge = new int[2];
    }
    public EdgePair(int start, int end){
        super();
        edge = new int[]{start, end};
    }
    public int getStart(){
        return edge[0];
    }

    public int getEnd(){
        return edge[1];
    }

    public void setEdge(int start, int end){
        edge[0] = start;
        edge[1] = end;
    }

    @Override
    public int compareTo(EdgePair edgePair) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        for(int i = 0; i < edge.length; i++){
            dataOutput.writeInt(edge[i]);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        edge = new int[2]; // all edges are length 2
        for(int i = 0; i < edge.length; i++){
            edge[i] = dataInput.readInt();
        }
    }
}
