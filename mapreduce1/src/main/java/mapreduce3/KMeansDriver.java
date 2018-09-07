package mapreduce3;


import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

// KMeansDriver contains:
// 1. read data and sent out data
// 2. computation step

public class KMeansDriver {
	public static void main(String[] args) throws Exception{
		// initialize configuration
		Configuration conf=new Configuration();
		// convert string to integer
		conf.setInt("numberOfCenters", Integer.parseInt(args[4]));
		// read parameters from terminal
		String inputfile=args[0];
		String inputPath=args[1];
		String centerPath=args[2];
		String clusterPath=args[3];
		// save it to global variable
		conf.set("centerPath", centerPath);

		
        double s = 0;
        double s1 = Double.MAX_VALUE; 
        double shold = 0.1; // threshold value
        int times = 0; // time of running the entire data set

        
        // convert txt file to sequence file because its faster
        writeToSeq(conf,inputPath,inputfile);
        // show users that the data set begin to generate centers
        System.out.println("Begin to generate centers");
        
        // initialize centers of the data set, assign it the dimension
        int dimension = centersInitial(conf, inputPath, centerPath);   

        // MRJob = MapReduce Job
        System.out.println("Generating centers for MRJob " + times + " successfully");

        // save the dimension to global variable
        conf.setInt("dimension", dimension);

        // initial the FileSystem (HDFS)
        // Java-based file system that provides scalable and reliable data storage
        FileSystem fs = FileSystem.get(conf);

        // define a job
        Job job = null;
        
        // iterative step to the optimize solution
        do {
        	// print every time it iterates
            System.out.println("MRJob-----------------------------" + times);
            
            // delete the temporary/previous clustering results
            fs.delete(new Path(clusterPath), true);

            // reference
            job = new Job(conf);

            // Hadoop Step
            // Java to Jar
            job.setJarByClass(KMeansDriver.class);
            // Map input file format
            job.setInputFormatClass(SequenceFileInputFormat.class);
            // state the class of the map
            job.setMapperClass(KMeansMapper.class);
            // output of the map, equivalent to string
            job.setMapOutputKeyClass(Text.class);
            // ID = date, Distance = distance between points to center point
            job.setMapOutputValueClass(IdAndDistance.class);
            // state the class of reduce
            job.setReducerClass(KMeansReducer.class);
            // output key 
            job.setOutputKeyClass(Text.class);
            // output value
            job.setOutputValueClass(Text.class);
            // output format class (sequence)
            job.setOutputFormatClass(SequenceFileOutputFormat.class);            

            // the path of input file
            SequenceFileInputFormat.addInputPath(job, new Path(inputPath));
            // the path of output file
            SequenceFileOutputFormat.setOutputPath(job, new Path(clusterPath));
            // wait for the job to complete ( one job = one map and one reduce)
            job.waitForCompletion(true);

            // if job complete
            if (job.waitForCompletion(true)) {
            	// delete temporary center variable in the file system
                fs.delete(new Path(centerPath), true);
                
                // generate new center points according to the results
                System.out.println("Begin to generate centers");

                // According the new center, return criterion function value 根据聚类结果生成新质心，并返回聚类结果的准则函数值
                
                // define new variable s2
                double s2 = newCenters(conf, inputPath, centerPath, clusterPath);
                // print the value of s2
                System.out.println("s2 = "+ s2);
                // as time increase
                times ++;
                // calculate the difference between s1(previous center point) and s2(current center point)
                s = Math.abs(s1 - s2);
                // print number of MapReduce jobs that have completed
                System.out.println("Generating centers for MRJob "+ times +" successfully");
                // print the value of s
                System.out.println("s = " + s);
                // set current value to previous, and repeat
                s1 = s2;
            }

          // if difference of the center point is greater than threshold 
          // continue if s > threshold, terminate if s < threshold
        } while (s > shold);
        
        // continue iterative step
        // write clusters result to local file
        // save
        writeClustersToLocalFile(conf, clusterPath);		
	}
	
	// ====================== SEQUENCE =====================
	// writeToSeq (convert input file to sequence file)
	 public static void writeToSeq(Configuration conf, String inputPath, String inputfile) throws IOException {
		 	// set uri ( unified resource )
		 	// define uri as string
	        String uri = inputPath;
	        // get the uri from file system
	        FileSystem fs=FileSystem.get(URI.create(uri),conf);
	        // define path
	        Path path= new Path(uri);
	        
	        // fs.conf.path = global configuration
	        // Text.class = key
	        // DoubleArray.class = value
	        SequenceFile.Writer Writer = new SequenceFile.Writer(fs,conf,path,Text.class,DoubleArray.class);

	 
	        // define file
	        File file = new File(inputfile);
	        // scanner
	        Scanner input = new Scanner(file);

	        // iterative step
	        while (input.hasNext()) {
	        	// read one line at a time
	            String[] line = input.nextLine().split("\t");
	            // key value = first term
	            Text key = new Text(line[0]);
	            // the rest of three are value
	            double[] data = new double[3];
	            
	            // High/Open
	            data[0] = Double.parseDouble(line[1]);
	            // Low/Open
	            data[1] = Double.parseDouble(line[2]);
	            // Close/Open
	            data[2] = Double.parseDouble(line[3]);
	            // Double Array = temporary output
	            DoubleArray value = new DoubleArray(data);
	            // append the key value pair
	            Writer.append(key, value);
	        }
	        // close the input and writer
	        input.close();
	        Writer.close();
	    }

		// ====================== Center Initial =====================
	    public static int centersInitial(Configuration conf, String inputPath, String centerPath) throws IOException {
	    	// initial dimension = 0
	        int dimension = 0;
	        // get inputPath from file system
	        FileSystem inputPathFs= FileSystem.get(URI.create(inputPath),conf);
	        // define path1
	        Path path1 = new Path(inputPath);
	        
	        // inputPathFs = sequence
	        // path1 = path
	        // conf = global configuration
	        SequenceFile.Reader inputPathReader = new SequenceFile.Reader(inputPathFs, path1, conf);

	        
	        // get centerPath from file system
	        FileSystem centerPathFs= FileSystem.get(URI.create(centerPath),conf);
	        // define path2
	        Path path2 = new Path(centerPath);
	        // send out the center
	        SequenceFile.Writer centerPathWriter = new SequenceFile.Writer(centerPathFs,conf,path2,Text.class,DoubleArray.class);
	        // key
	        Text key = (Text) ReflectionUtils.newInstance(Text.class, conf);
	        // value
	        DoubleArray value = (DoubleArray) ReflectionUtils.newInstance(DoubleArray.class, conf);

	        // try, to prevent error
	        try {
	        	// define k = number of cluster
	            int k = 0;
	            // start to read
	            while (inputPathReader.next(key, value)) {
	            	// improvement: randomly select center point
	            	// if k is less than the number of center points given 5
	                if (k < conf.getInt("numberOfCenters", 5)) {
	                	// append center point
	                    centerPathWriter.append(new Text(String.valueOf(k)), value);
	                    // define dimension and get the length
	                    dimension = value.get().length;
	                    // print out 5 center points with three characteristics
	                    System.out.println("center\t"+String.valueOf(k)+"\t"+"("+(value.get())[0]+","+(value.get())[1]+","+(value.get())[2]+")");
	                } else {
	                    break;
	                }
	                //++
	                k = k + 1;
	            }
	        // if any error
	        } finally {
	        	// close stream
	            IOUtils.closeStream(inputPathReader);
	        }
	        // close writer
	        centerPathWriter.close();
	        return dimension;
	    }

		// ====================== New Center =====================
	    public static double newCenters(Configuration conf, String inputPath, String centerPath, String clusterPath) throws IOException {

	        double s = 0;
	        String[] clusters = new String[conf.getInt("numberOfCenters", 5)];
	        System.out.println(conf.getInt("numberOfCenters",5));
	        DoubleArray[] centers = new DoubleArray[conf.getInt("numberOfCenters", 5)];
	        System.out.println("----------------------------");
	        System.out.println(centers);
	        System.out.println(centers.length);
	        // i = number of center point
	        for (int i = 0; i < centers.length; i++) {
	        	// 3 dimension 
	            double[] temp = new double[conf.getInt("dimension", 1)];
	            // k = dimension
	            for (int k = 0; k < temp.length; k++) 
	            temp[k] = 0;
	            // for 5 center point
	            centers[i] = new DoubleArray(temp);
	        }

	        // part-r-00000 = output of clusterPath
	        FileSystem clusterPathFs =  FileSystem.get(URI.create(clusterPath+"/part-r-00000"), conf);
	        Path path = new Path(clusterPath+"/part-r-00000");
	        
	        // warning
	        @SuppressWarnings("deprecation")
			SequenceFile.Reader clusterReader = new SequenceFile.Reader(clusterPathFs, path, conf);
	        // initial key and value
	        Text clusterKey = (Text) ReflectionUtils.newInstance(Text.class, conf);
	        Text clusterValue = (Text) ReflectionUtils.newInstance(Text.class, conf);
	        // k = counter variable
	        int k = 0;
	        try {
	        	// start to read, read key and value
	            while (clusterReader.next(clusterKey, clusterValue)) {
	                clusters[Integer.parseInt(clusterKey.toString())] = clusterValue.toString();
	                System.out.println("***********");
	                System.out.println(clusterValue.toString());
	                // cluster value = distance from point to center point, convert to string
	                int indexOfDistance = clusterValue.toString().lastIndexOf(",") + 1;
	                // add up
	                double sumOfDistance = Double.parseDouble(clusterValue.toString().substring(indexOfDistance));
	                // redefine s = total distance
	                s = s + sumOfDistance;
	                // ++
	                k = k + 1;
	            }
	        } finally {
	        	// close stream
	            IOUtils.closeStream(clusterReader);
	        }
      
			// input path again
	        FileSystem inputPathFs= FileSystem.get(URI.create(inputPath),conf);
	        Path path1 = new Path(inputPath);
	        SequenceFile.Reader inputPathReader = new SequenceFile.Reader(inputPathFs, path1, conf);
	        Text inputKey = (Text) ReflectionUtils.newInstance(Text.class, conf);
	        DoubleArray inputValue = (DoubleArray) ReflectionUtils.newInstance(DoubleArray.class, conf);

	        try {
	        	// start to read key and value line by line
	            while (inputPathReader.next(inputKey, inputValue)) {
	                for (int i = 0; i < conf.getInt("numberOfCenters", 5); i++) {
	                	System.out.println("+++++++++++++++++++++");
	                	System.out.println(clusters[i]);
	                	// i = 0, i repeat 5 times
	                	if(clusters[i] != null) {
	                		if (clusters[i].indexOf(inputKey.toString()+",") == 0 
	                		// or
	                        || clusters[i].indexOf(","+inputKey.toString()+",") > 0 
	                        // or
	                        || clusters[i].indexOf(","+inputKey.toString()) == clusters[i].length()-(","+inputKey.toString()).length()) {
	                		// determine whether the value is valid, add the center
	                		// SUM
	                        centers[i].plus(inputValue);
	                    }
	                	}
	                }
	            }
	        } finally {
	        	// close stream
	            IOUtils.closeStream(inputPathReader);
	        }
	        // average the SUM
	        for (int i = 0; i < conf.getInt("numberOfCenters", 5); i++) {
	        		// calculate the average from the SUM centers[i]
	            	if(clusters[i]!=null) {
	        		centers[i].averageN(clusters[i].split(",").length);
	        		// print the 5 center points with 3 dimensions
	            	System.out.println("center\t"+String.valueOf(i)+"\t"+"("+(centers[i].get())[0]+","+(centers[i].get())[1]+","+(centers[i].get())[2]+")");
	            	}
	            	}
	        // save the output of the center point
	        FileSystem centerPathFs= FileSystem.get(URI.create(centerPath),conf);
	        Path path2 = new Path(centerPath);
	        SequenceFile.Writer centerPathWriter = new SequenceFile.Writer(centerPathFs,conf,path2,Text.class,DoubleArray.class);
	        // append the value of center points
	        for (int i = 0; i < conf.getInt("numberOfCenters", 5); i++) {
	        	// writer append to save
	            centerPathWriter.append(new Text(String.valueOf(i)), centers[i]);
	        }
	        // close writer
	        centerPathWriter.close();
	        return s;
	    }

	    // previously it was save to HDFS, now after computed the average of the center points
	    // write the cluster to local file
	    
		// ====================== Writer Cluster to Local File =====================
	    public static void writeClustersToLocalFile(Configuration conf, String clusterPath) throws IOException {
	    	// local file path
	        File file = new File("s3://mapreduce3-sijia/output.txt");
	        // define output
	        PrintWriter output = new PrintWriter(file);
	        // /part-r-00000 = name of cluster file
	        FileSystem clusterPathFs =  FileSystem.get(URI.create(clusterPath+"/part-r-00000"), conf);
	        Path path = new Path(clusterPath+"/part-r-00000");
	        // sequence file to read the cluster file 
	        SequenceFile.Reader clusterReader = new SequenceFile.Reader(clusterPathFs, path, conf);
	        //  state the key and value
	        Text clusterKey = (Text) ReflectionUtils.newInstance(Text.class, conf);
	        Text clusterValue = (Text) ReflectionUtils.newInstance(Text.class, conf);
	        try {
	        	// start to read and send out 
	            while (clusterReader.next(clusterKey, clusterValue)) {
	            	// split with , comma
	                String[] line = clusterValue.toString().split(",");
	                // for every i = number of row
	                for (int i = 0; i < line.length - 1; i++) {
	                	// print the value with \t in between
	                    output.print(clusterKey.toString()+"\t");
	                    // save to local file
	                    output.print(line[i]+"\n");
	                }
	            }
	        } finally {
	        	// close stream
	            IOUtils.closeStream(clusterReader);
	        }
	        // close
	        output.close();
	    }
}


