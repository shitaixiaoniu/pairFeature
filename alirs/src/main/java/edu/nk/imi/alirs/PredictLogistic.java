package edu.nk.imi.alirs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Locale;

import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.classifier.sgd.CsvRecordFactory;
import org.apache.mahout.classifier.sgd.LogisticModelParameters;
import org.apache.mahout.classifier.sgd.OnlineLogisticRegression;
import org.apache.mahout.classifier.sgd.TrainLogistic;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

public class PredictLogistic {
	private double threshold;
	public PredictLogistic(double threshold)
	{
		this.threshold = threshold;
	}
	public  PredictLogistic()
	{
		this(0.5);
	}
	public  void predict(String inputFile ,String modelFile, String outFile) throws Exception
	{
		LogisticModelParameters lmp = LogisticModelParameters.loadFrom(new File(modelFile));
		CsvRecordFactory csv = lmp.getCsvRecordFactory();
		OnlineLogisticRegression lr = lmp.createRegression();
		FileWriter writer = new FileWriter(outFile, true);
		BufferedReader in =open(inputFile);
		String line = in.readLine();
		csv.firstLine(line);
		line = in.readLine();
		while (line != null) {
			Vector v = new SequentialAccessSparseVector(lmp.getNumFeatures());
			int target = csv.processLine(line, v);
			double score = lr.classifyScalar(v);
			int predictClass = score > threshold ? 1 : 0;
			String str = score+","+predictClass;
			writer.write(str+"\r\n");
			line = in.readLine();
        }
		writer.close();
	}
	static BufferedReader open(String inputFile) throws IOException {
	    InputStream in;
	    try {
	      in = Resources.getResource(inputFile).openStream();
	    } catch (IllegalArgumentException e) {
	      in = new FileInputStream(new File(inputFile));
	    }
	    return new BufferedReader(new InputStreamReader(in, Charsets.UTF_8));
	  }
}
