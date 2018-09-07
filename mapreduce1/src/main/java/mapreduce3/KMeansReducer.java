package mapreduce3;


import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansReducer extends Reducer<Text, IdAndDistance, Text, Text>{
	// input key, input value, output key, output value

	
	// ====================== REDUCE =====================
    public void reduce(Text key, Iterable<IdAndDistance> values, Context context) throws IOException, InterruptedException {
    	// SUM
        double sumOfDistance = 0;
        // iterator 
        Iterator<IdAndDistance> ite = values.iterator();
        // initialize cluster as string
        String cluster = "";
        
        // start to iterate
        while (ite.hasNext()) {
        	// temporary value of id and distance 
        	// clone value of iterative value
            IdAndDistance temp = WritableUtils.clone(ite.next(), context.getConfiguration());
            
            // if length cluster is greater than 0, add a comma
            if (cluster.length() > 0) cluster = cluster + ",";
            // get id = date
            cluster = cluster + temp.getId();
            // add up
            sumOfDistance = sumOfDistance + temp.getDistance();
        }
        
        // add the sumofDistance to cluster
        cluster = cluster + "," + String.valueOf(sumOfDistance);
        // output
        context.write(key, new Text(cluster));
    }
}