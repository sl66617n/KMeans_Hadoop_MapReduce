## K-Means Clustering in Java using Hadoop MapReduce

This project uses Amazon Web Service (AWS) in order to do a cloud based Map Reduce K-Means Clustering. The MapReduce program that is being used is Hadoop.

There are three parts:

   1.  What is MapReduce and what it does?
   2.  Java code
   3.  Executing/loading task on to the cloud infrastructure (AWS) 

------



### History of MapReduce

History of MapReduce originated in the development of web search engines primarily in Google’s research and development labs. It became clear early on in the development of search engines that there was gonna need to be new infrastructure built in order to handle enormous amounts of  memory what became to known as big data. And this is so much data that it doesn’t fit within memory at one time and at one computer. Hence MapReduce is about handling enormous amounts of data and doing processing tasks on that data. MapReduce is a data flow architecture, it has a particular way in which that it manipulates data. Many problems can be solved if it can be fit into the style of MapReduce jobs.

---

### Steps of MapReduce

MapReduce has two steps that are Map step and Reduce step, it is a parallel task executed on many different computers and one of those tasks that a developer can design is the map process. There are different map processes that are introduced into a MapReduce job, each of those map processes works on data independently from the enormous set of data that you’ve got as a base set. After the map process is done, Hadoop shuffles data around to several reduce processes. Those operate in parallel in conjunction with the map processes. Finally when the reduce process is done, you have some transformation of the initial data that represents a change or some analysis that you want done on that large amount of data. 

Hadoop is an implementation of MapReduce that is available on AWS. Each map process takes one of those incoming units and it transforms it into multiple key and value pairs. Then a whole bunch of key value pairs gets passed out of the map process along the ways. The keys are all separated, and the values are different as well. Then the hadoop mapreduce architecture takes those and it shuffles them around in a particular way that is the strength of this mapreduce data flow into a bucket. The way in which they are rearranged is such that all of the key-value pairs that came out of the map process get collated into a key and list of  values where the key was the same then the value associated with that key is going to be appended to the end of the list that is delivered to the reduce process.

---

### Types of Values

The type of the value and key have to be descendants of the writable class, they have to have the characteristics of the writable class. Hadoop comes with many very common classes, which are just variants of the standard Java classes that are available but different names. The following are all data structures and types that have been wrapped in the writable wrapper so that you can pass them around effectively within Hadoop:

* IntWritable
* DoubleWritable
* LongWritable
* BooleanWritable
* Text
* ObjectWritable
* MapWritable

These are all Hadoop version of integer, double and long numeric types. Text is a generic string that can be pass around that implements writable. ObjectWritable can take a serializable object. MapWritable can take Java map data structure.


---

### Java Code

Check out src/main/java/mapReduce3 for complete code snippet.

* KMeansDriver.java
* KMeansMapper.java
* KMeansReducer.java
* IdAndDistance.java
* DoubleArray.java


Some sample code are shown as following:

* KMeansDriver.java
```java
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
...
```
* KMeansMapper.java

```java
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
```
* KMeansReducer.java
```java
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
```
* IdAndDistance.java
```java
	// read ( for inheritance)
	public void readFields(DataInput in) throws IOException {
		// UTF-8
		id=in.readUTF();
		distance=in.readDouble();
	}

	// write ( for inheritance)
	public void write(DataOutput out) throws IOException {
		out.writeUTF(id);
		out.writeDouble(distance);
	}
}
```
* DoubleArray.java
```java
	// compute distance between one double array to another 
	public double distanceTo(DoubleArray point) {
		double[] data1 = point.get();
		double distance = 0;
		for (int i = 0; i < data.length; i++) {
			distance = distance + Math.pow(data[i] - data1[i], 2);
		}
		return distance;
	}
```


---

### Deployment

![image](https://user-images.githubusercontent.com/30872011/45192882-1b7e7900-b219-11e8-8783-f57d6b71528c.png)

 - [x] EC2 - Create Key Pairs named mapreduce3.
![image](https://user-images.githubusercontent.com/30872011/45193022-12da7280-b21a-11e8-8854-e54995801268.png)
 
 
 - [x] S3 - Create a bucket, and upload dataset.txt and kmeans.jar in the bucket you've created.
![image](https://user-images.githubusercontent.com/30872011/45193001-efafc300-b219-11e8-9f54-4a2634c59931.png)
 
 - [x] EMR - Create Cluster, Add Step, choose the kmeans.jar, add the arguments.
![image](https://user-images.githubusercontent.com/30872011/45193126-83818f00-b21a-11e8-93fd-c00112c2fb51.png)
 - [x] Shutdown/Terminate on AWS, Delete files uploaded

For visualization, export the output.txt, manipulate data, plot with any tools you preferred.

(For small dataset that can be included in .csv, simply use vlookup function in excel for manipulation.)


---

### DataSet

I used [Complete Cryptocurrency Market History Dataset](https://www.kaggle.com/taniaj/cryptocurrency-market-history-coinmarketcap):link: from Kaggle.


and reduce the dimension to High/Open, Low/Open, Close/Open from regular OHLC.


Below is an sample clustering for S&P 500:
![image](https://user-images.githubusercontent.com/30872011/45195367-2f2fdc80-b225-11e8-8f24-f210b28fb4d1.png)


---

### Version

Java - 1.8

Hadoop - 2.8.4

