package mapreduce3;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

// ====================== Double Array =====================

// three dimensions(characteristics) are save to Double Array
// purpose: define a new data structure which contains the three dimensions


public class DoubleArray implements Writable {
	private double[] data;
	public DoubleArray() {
	}

	
	// setter and getter
	public DoubleArray(double[] data) {
		set(data);
	}
	public void set(double[] data) {
		this.data = data;
	}
	public double[] get() {
		return data;
	}

		
	// write (for inheritance)
	// define how you want to write the file
	public void write(DataOutput out) throws IOException {
		int length = 0;
		if (data != null) {
			length = data.length;
		}
		out.writeInt(length);

		for (int i = 0; i < length; i++) {
			out.writeDouble(data[i]);
		}
	}

	
	// read (for inheritance)
	// define how you want to read the file
	public void readFields(DataInput in) throws IOException {
		int length = in.readInt();
		data = new double[length];
		for (int i = 0; i < length; i++) {
			data[i] = in.readDouble();
		}
	}

		
	// compute distance between one double array to another 
	public double distanceTo(DoubleArray point) {
		double[] data1 = point.get();
		double distance = 0;
		for (int i = 0; i < data.length; i++) {
			distance = distance + Math.pow(data[i] - data1[i], 2);
		}
		return distance;
	}

	
	// plus ( add function for two double array)
	public void plus(DoubleArray point) {
		double[] data1 = point.get();
		for (int i = 0; i < data.length; i++) {
			data[i] = data[i] + data1[i];
		}
	}
	
	// average ( average for three characteristics in the double arrary)
	public void averageN(int n) {
		for (int i = 0; i < data.length; i++) {
			data[i] = data[i]/n;
		}
	}
}