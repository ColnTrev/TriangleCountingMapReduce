import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;


/**
 * Created by colntrev on 2/18/18.
 */
public class UniqueTriangleMap extends Mapper<Text,Text,Text,Text>{
    static Text sorted = new Text();
    @Override
    public void map(Text triangle, Text value, Context context) throws IOException,InterruptedException {
        String[] edges = StringUtils.split(triangle.toString()," ");
        Arrays.sort(edges);
        sorted.set(edges[0] + ',' + edges[1] + ',' + edges[2]);
        context.write(sorted,value);
    }
}
