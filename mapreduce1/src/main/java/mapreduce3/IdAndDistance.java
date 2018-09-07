package mapreduce3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

// ====================== ID and Distance =====================

// id = id of the center point
// distance = distance between every point to the center point

// purpose: serve as a temporary variable to connect the distance and id


public class IdAndDistance implements Writable{
	//initialize id as string, and distance as double
	private String id;
	private double distance;
	
	
	//setter and getter
	public void set(String id,double distance) {
		this.id=id;
		this.distance=distance;
	}
	public IdAndDistance() {	
	}
	public IdAndDistance(String id,double distance){
		set(id,distance);
	}
	public String getId(){
		return id;
	}
	public double getDistance() {
		return distance;
	}
	
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
