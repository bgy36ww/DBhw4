import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;


public class LeftoverReducer extends Reducer<IntWritable, Node, IntWritable, Node> {
    public static double alpha = 0.85;
    public void reduce(IntWritable nid, Iterable<Node> Ns, Context context) throws IOException, InterruptedException {
        //all the stuff is stored in counters
        //so first thing is to pull them out
        Configuration config=context.getConfiguration();
        //pull size from config
        long size=Long.parseLong(config.get("size"));
        //pull leftover from config
        double leftover=Long.parseLong(config.get("leftover"))/100000000;
        //grab the current node
        Node n=Ns.iterator().next();
        //calculate the new pagerank
        double npr=alpha/size+(1-alpha)*(leftover/size+n.getPageRank());
        //set it to the node
        n.setPageRank(npr);
        //emit(nid,node)
        context.write(nid,n);
        
        
        

    }
}
