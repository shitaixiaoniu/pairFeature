package edu.nk.imi.alirs.process;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Map.Entry;

import edu.nk.imi.util.UitlReader;

public class TopItems {
	public void getTopItems(String predictPath ,String resPath, int topN) throws Exception
	{
		UitlReader util = new UitlReader();
		util.init(predictPath);
		String line = null;
		int count=0;
		HashMap<String,PriorityQueue<PredictItem>> topMap = new HashMap<String, PriorityQueue<PredictItem>>();
		while((line = util.nextLine())!=null && !line.equals(""))
		{
			String[] parts = line.split(",");
			String userid = parts[0];
			String itemid = parts[1];
			double score = Double.parseDouble(parts[2]);
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
		util.closeReader();
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
	}

}
