import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/**
 * Created by colntrev on 2/18/18.
 */
public class GraphEdgeReduce extends Reducer<IntWritable,IntWritable,EdgePair,IntWritable> {
    EdgePair pair = new EdgePair();
    IntWritable connector = new IntWritable();
    @Override
    protected void reduce(IntWritable node, Iterable<IntWritable> edges, Context context) throws IOException,InterruptedException {

        List<Integer> cache = new ArrayList<>(); // add values from iterable here since iterable passes once
        //write edges in K,V pairs with a null connector
        // example [2,1],(-)
        connector.set(0);
        for(IntWritable edge : edges){
            cache.add(edge.get());
            pair.setEdge(node.get(), edge.get());
            context.write(pair,connector);
        }
        Collections.sort(cache);
        //write edges with V,V pairs with their K connection
        // example [1,3], [2]
        for(int i = 0; i < cache.size(); i++){
            for(int j = i + 1; j < cache.size(); j++){
                    pair.setEdge(cache.get(i), cache.get(j));
                    context.write(pair, node);
            }
        }
    }
}
