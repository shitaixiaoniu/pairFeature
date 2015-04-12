package edu.nk.imi.alirs.process;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import org.apache.mahout.classifier.sgd.CsvRecordFactory;
import org.apache.mahout.classifier.sgd.LogisticModelParameters;
import org.apache.mahout.classifier.sgd.OnlineLogisticRegression;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;

import edu.nk.imi.util.UitlReader;

public class MergeAndPredictLR {
	

	private double threshold;
	public MergeAndPredictLR(double threshold)
	{
		this.threshold = threshold;
	}
	public MergeAndPredictLR()
	{
		this(0.5);
	}

	String defaultpairfeature = "-0.08776667,-0.00781667,-0.00728889,-0.00112727,-0.01935591," +
			"-0.00702857,-0.00401053,-0.00094444,-0.00054643,-0.00040292,-0.01751548,-0.51484906," +
			"-0.53236457,0,0,0,0";
	
	int numperfile = 666292;
	
	public void mergeFeatureAndPredict(String pairPath,String testPairPath,
			String itempath,String userpath,String modelPath,String resPath,int topN)
	{
		System.out.println("-----------filterRecord start----------");
		try {
			
			UitlReader util = new UitlReader();
			
			//pair
			util.init(pairPath);
			String line = null;
			HashMap<String, String> map_pair = new HashMap<String, String>();
			while((line = util.nextLine())!=null && !line.equals(""))
			{
				String id = line.substring(0, line.indexOf(","));
				String feature = line.substring(line.indexOf(",")+1, line.length());
				map_pair.put(id,feature);
			}
			util.closeReader();
			System.out.println("read pair feature to memory done!");
			
			//item
			util.init(itempath);
			line = null;
			HashMap<String, String> map_item = new HashMap<String, String>();
			while((line = util.nextLine())!=null && !line.equals(""))
			{
				String id = line.substring(0, line.indexOf(","));
				String feature = line.substring(line.indexOf(",")+1, line.length());
				
				map_item.put(id,feature);
			}
			util.closeReader();
			System.out.println("read item feature to memory done!");

			//user
			util.init(userpath);
			line = null;
			HashMap<String, String> map_user = new HashMap<String, String>();
			while((line = util.nextLine())!=null && !line.equals(""))
			{
				String id = line.substring(0, line.indexOf(","));
				String feature = line.substring(line.indexOf(",")+1, line.length());
				
				map_user.put(id,feature);
			}
			util.closeReader();
			System.out.println("read user feature to memory done!");
			
			LogisticModelParameters lmp = LogisticModelParameters.loadFrom(new File(modelPath));
			CsvRecordFactory csv = lmp.getCsvRecordFactory();
			OnlineLogisticRegression lr = lmp.createRegression();
			//第一行变量名 - LR使用
			String firstline = "";
			for(int i=1;i<=66;i++)
			{
				firstline = firstline + "f"+i+",";
			}
			firstline = firstline+"y";
			csv.firstLine(firstline);
			System.out.println("-----------fisrt line done----------");
			
			HashMap<String,PriorityQueue<PredictItem>> topMap = new HashMap<String, PriorityQueue<PredictItem>>();
	
			util.init(testPairPath);
			line = null;
			int count=0;
			while((line = util.nextLine())!=null && !line.equals(""))
			{
				
				//拼接特征
				String featureLine = null;         
				
				String userid = line.substring(0, line.indexOf(","));
				String itemid = line.substring(line.indexOf(",")+1,line.length());
				
				String lineid = userid+"_"+itemid;
				if(map_pair.containsKey(lineid))
				{
					featureLine = map_pair.get(lineid)+","+count%2+"\r\n";
					//map_pair.remove(lineid);
				}
				else
				{
					featureLine = defaultpairfeature+","+map_user.get(userid)+","
							+map_item.get(itemid)+","+count%2+"\r\n";
				}
				//进行预测
				Vector v = new SequentialAccessSparseVector(lmp.getNumFeatures());
				int target = csv.processLine(line, v);
				double score = lr.classifyScalar(v);
				int predictClass = score > threshold ? 1 : 0;
				//预测是正例 
				if(predictClass == 1)
				{
					if(!topMap.containsKey(userid))
					{
						topMap.put(userid, new PriorityQueue<PredictItem>(topN,new PredictItemComparator()));
					}
					if(topMap.get(userid).size() < topN)
					{
						topMap.get(userid).add(new PredictItem(itemid, score));
					}
					else
					{
						if(topMap.get(userid).peek().getScore() < score)
						{
							topMap.get(userid).poll();
							topMap.get(userid).add(new PredictItem(itemid, score));
						}
						
					}
				}
				count++;
				if( count%1000 == 0)
				{
					System.out.println(count+" done!");
				}
			}
			util.closeReader();
			System.out.println("iterate lines done!");
			//结果文件
			FileWriter writer = new FileWriter(resPath, true);
			writer.write("user_id,item_id\r\n");
			for (Entry<String, PriorityQueue<PredictItem>> entry : topMap.entrySet()) {
				for (Iterator<PredictItem> iterator = entry.getValue().iterator(); iterator
						.hasNext();) {
					PredictItem pi = (PredictItem) iterator.next();
					writer.write(entry.getKey()+","+pi.getItemId()+"\r\n");
					
				}	
			}
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("-----------filterRecord end----------");
	}
	
}
