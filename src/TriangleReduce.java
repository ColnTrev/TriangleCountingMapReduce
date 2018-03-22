import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TriangleReduce extends Reducer<EdgePair,IntWritable,Text,Text> {
    static final Text BLANK = new Text("");
    @Override
    protected void reduce(EdgePair node, Iterable<IntWritable> connectors, Context context) throws IOException,InterruptedException {
        List<Integer> cache = new ArrayList<>();
        boolean seenZero = false; // zero represents null or no connecting node;
        for(IntWritable connector : connectors){
            int c = connector.get();
            if(c == 0){
                seenZero = true;
            } else {
                cache.add(c);
            }
        }

        if(seenZero){
            if(!cache.isEmpty()){
                Text triangle = new Text();
                for(int c : cache) {
                    String result = node.getStart() + "," + node.getEnd() + "," + c;
                    triangle.set(result);
                    context.write(triangle, BLANK);
                }
            }
        }
        return; // no triangles in this reducer
    }
}
