

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class TriangleMap extends Mapper<EdgePair,IntWritable,EdgePair,IntWritable>{

    @Override
    public void map(EdgePair pair, IntWritable connector, Context context) throws IOException,InterruptedException {
        context.write(pair, connector);
    }
}
