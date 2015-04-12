package edu.nk.imi.alirs;

import java.io.FileWriter;

import edu.nk.imi.alirs.util.UitlReader;

public class DataTransform {

	public void getFinalRes(String pairFile ,String predictFile,String resFile) throws Exception
	{
		FileWriter writer = new FileWriter(resFile, true);
		writer.write("user_id,item_id"+"\r\n");
		UitlReader utilTest = new UitlReader();
		utilTest.init(pairFile);
		
		UitlReader utilPredict = new UitlReader();
		utilPredict.init(predictFile);
		String lineTest = utilTest.nextLine();
		String linePredict = utilPredict.nextLine();
		while(lineTest != null && linePredict != null)
		{
			String[] partsPredict =  linePredict.split(",");
			//正例
			if(partsPredict[1] .equals("1"))
			{
				writer.write(lineTest+"\r\n");
			}
			lineTest = utilTest.nextLine();
			linePredict = utilPredict.nextLine();
		}
		if(lineTest!= null || linePredict != null)
		{
			throw new Exception("pair num and predict num not match");
		}
		utilTest.closeReader();
		utilPredict.closeReader();
		writer.close();
	}
}
