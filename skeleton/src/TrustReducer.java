import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

public class TrustReducer extends Reducer<IntWritable, NodeOrDouble, IntWritable, Node> {
    public void reduce(IntWritable key, Iterable<NodeOrDouble> values, Context context)
        throws IOException, InterruptedException {
        //create pagerank accumulator
        double pgacc=0;
        //create dummy node holder for current node
        Node N=null;
        //make a iterator to iterate through values
        for (NodeOrDouble itr:values){
            //if it's a node
            if(itr.isNode()){
                //set N as node
                N=itr.getNode();
            }else
            {
                //accumulate rank;
                pgacc+=itr.getDouble();
            }
        }
        //set pagerank of current node
        N.setPageRank(pgacc);
        //emit current node emit(nid,node)
        context.write(key,N);
        
        
    }
}
