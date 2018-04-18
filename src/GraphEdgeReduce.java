import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;


/**
 * Created by colntrev on 2/18/18.
 */
public class GraphEdgeReduce extends Reducer<IntWritable,IntWritable,EdgePair,IntWritable> {
    @Override
    protected void reduce(IntWritable node, Iterable<IntWritable> edges, Context context) throws IOException,InterruptedException {
        Map<EdgePair,List<IntWritable>> duplicates = new HashMap<>();
        List<Integer> cache = new ArrayList<>(); // add values from iterable here since iterable passes once
        //write edges in K,V pairs with a null connector
        // example [2,1],(-)
        for(IntWritable edge : edges){
            IntWritable connector = new IntWritable(0);
            cache.add(edge.get());
            EdgePair pair = new EdgePair(node.get(), edge.get());
            if(!duplicates.containsKey(pair)){
                duplicates.put(pair, new ArrayList<>());
            }
            if(!duplicates.get(pair).contains(connector)) {
                duplicates.get(pair).add(connector);
                context.write(pair, connector);
            }
        }
        Collections.sort(cache);

        //write edges with V,V pairs with their K connection
        // example [1,3], [2]

        for(int i = 0; i < cache.size(); i++){
            for(int j = i + 1; j < cache.size(); j++){
                EdgePair pair = new EdgePair(cache.get(i), cache.get(j));
                if(!duplicates.containsKey(pair)){
                    duplicates.put(pair, new ArrayList<>());
                }
                if(!duplicates.get(pair).contains(node)) {
                    duplicates.get(pair).add(node);
                    context.write(pair, node);
                }
            }
        }
    }
}
