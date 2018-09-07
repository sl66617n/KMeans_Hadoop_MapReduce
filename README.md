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

MapReduce has two steps that are Map step and Reduce step, it is a parallel task executed on many different computers and one of those tasks that a developer can design is the map process. There are different map processes that are introduced into a MapReduce job, each of those map processes works on data independently from the enormous set of data that you’ve got as a base set. After the map process is done, Hadoop shuffles data around to several reduce processes. Those operate in parallel in conjunction with the map processes. Finally when the reduce process is done, you have some transformation of the initial data that represents a change or some analysis that you want done on that large amount of data. Hadoop is an implementation of MapReduce that is available on AWS. Each map process takes one of those incoming units and it transforms it into multiple key and value pairs. Then a whole bunch of key value pairs gets passed out of the map process along the ways. The keys are all separated, and the values are different as well. Then the hadoop mapreduce architecture takes those and it shuffles them around in a particular way that is the strength of this mapreduce data flow into a bucket. The way in which they are rearranged is such that all of the key-value pairs that came out of the map process get collated into a key and list of  values where the key was the same then the value associated with that key is going to be appended to the end of the list that is delivered to the reduce process.

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

Check out src/main/java/mapReduce3

* KMeansDriver.java
* KMeansMapper.java
* KMeansReducer.java
* IdAndDistance.java
* DoubleArray.java


---

### Deployment

![image](https://user-images.githubusercontent.com/30872011/45192824-ac088980-b218-11e8-9b26-7993e2d13501.png)

 - [x] EC2 - Create Key Pairs named MapReduce-sijia
 <img width="781" alt="keypair" src="https://user-images.githubusercontent.com/30872011/45192788-811e3580-b218-11e8-8823-4236492eb419.png">
 
 - [x] S3 - Upload dataset.txt and kmeans.jar
 - [x] EMR - Create Cluster, Add Step, choose the kmeans.jar, add the arguments
 - [x] Shutdown/Terminate on AWS, Delete files uploaded

For visualization, export the output.txt, manipulate data, plot with any tools you preferred.

(For small dataset that can be included in .csv, simply use vlookup function in excel for manipulation.)


---

### DataSet

I used [Complete Cryptocurrency Market History Dataset](https://www.kaggle.com/taniaj/cryptocurrency-market-history-coinmarketcap):link: from Kaggle.



and reduce the dimension to High/Open, Low/Open, Close/Open from regular OHLC.

---

### Version

Java - 1.8

Hadoop - 2.8.4

