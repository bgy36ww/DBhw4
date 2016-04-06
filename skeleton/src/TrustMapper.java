import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;


public class TrustMapper extends Mapper<IntWritable, Node, IntWritable, NodeOrDouble> {
    public void map(IntWritable key, Node value, Context context) throws IOException, InterruptedException {
        //emit current node for reducer to use
        context.write(key, NodeOrDouble(value));
        //increment the count of the total nodes
        context.getCounter("Counters", "Count").increment(1);
        //check if there is any outgoing path for this node. In other words, is this a leftovernode
        if (value.outgoingSize()==0){
        //calculate amount of rank that needs to distributed among other nodes
        double inc=value.getPageRank();
        //counter can not be double
        //so let's make them into long and treat them later
        //double * 100000000 can prevent data lost
        context.getCounter("Counters","Amout").increment((long)(inc*100000000));
        }
        else{
            //create iterator that can iterate through the paths
            Iterator itr=value.iterator();
            //calculate the emit rank to other connected node
            double erank=value.getPageRank()/value.outgoingSize();
            //go through all of the paths
            while (itr.hasNext()){
                //create output node id
                IntWritable nid=IntWritable((int)itr.next());
                //emit nid,p
                context.write(nid,erank);
            }}
        
        
    }
}
