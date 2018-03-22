/**
 * Created by colntrev on 2/18/18.
 */
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class GraphEdgeMap extends Mapper<IntWritable,Text,IntWritable,IntWritable>{
    IntWritable key;
    IntWritable val;
    @Override
    public void map(IntWritable k, Text v, Context context) throws IOException,InterruptedException {
        String edge = v.toString().trim();
        String[] nodes = StringUtils.split(edge, " ");
        Integer start = Integer.parseInt(nodes[0]);
        Integer end   = Integer.parseInt(nodes[1]);
        key.set(start);
        val.set(end);
        context.write(key, val);
        context.write(val,key);
    }
}
