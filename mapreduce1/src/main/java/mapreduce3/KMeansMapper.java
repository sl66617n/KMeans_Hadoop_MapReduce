package mapreduce3;


import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;

// Mapper
public class KMeansMapper extends Mapper<Text,DoubleArray,Text,IdAndDistance>{
	// input key, input value, output key, output value
	// initial center as double array as null
	private DoubleArray[] centers=null;
	
	// setup initialization
	protected void setup(Context context) throws IOException,InterruptedException{
		// super/inheritance the context
		super.setup(context);
		// initialize the configuration context
		Configuration conf=context.getConfiguration();
		// read the number of centers, initialize a double array
		centers=new DoubleArray[conf.getInt("numberOfCenters", 5)];
		// get center path
		String centerPath=conf.get("centerPath");
		
		// initialize the file system path
		FileSystem fs=FileSystem.get(URI.create(centerPath),conf);
		Path path=new Path(centerPath);
		SequenceFile.Reader reader=new SequenceFile.Reader(fs, path,conf);
		// key and value
		Text key=(Text)ReflectionUtils.newInstance(Text.class,conf);
		DoubleArray value=(DoubleArray)ReflectionUtils.newInstance(DoubleArray.class, conf);
		
		try {
			// start to read
			while(reader.next(key,value)){
				// read center point
				int index=Integer.parseInt(key.toString());
				// get array of the three dimensions
				double[] array=value.get();
				// 5 center points with 3 dimensions
				centers[index]=new DoubleArray(array);
			}
		}finally {
			// close stream
			IOUtils.closeStream(reader);
		}
	}
	
	// ====================== MAP=====================
	public void map(Text key,DoubleArray value,Context context) throws IOException,InterruptedException {
		// minDistance = distance between each point and center point
        double minDistance = Double.MAX_VALUE; // initialize a max value
        // nearest center point
        int nearestCenter = 0;
        // compute which one of the 5 center points is the nearest
        for (int i = 0; i < centers.length; i++) {
        	// if distance between point to center point is less the the minimal distance
            if (value.distanceTo(centers[i]) < minDistance) {
                nearestCenter = i;
                // set to the point and calculate the minimal distance
                minDistance = value.distanceTo(centers[i]);
            }
        }
        // output distance and nearest center point
        context.write(new Text(String.valueOf(nearestCenter)), new IdAndDistance(key.toString(),minDistance));
    }
}
