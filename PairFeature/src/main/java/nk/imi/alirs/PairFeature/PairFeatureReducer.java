package nk.imi.alirs.PairFeature;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;

public class PairFeatureReducer implements Reducer {

//	private final String timeSplitPointStr = "2014-12-19 00";
	private Date splitPointDate ;

	private final String timeDefaultStr = "2014-11-18 00";
	private Date defaultDate;
	
	//不可能出现的日期
	private final String timeFlagStr = "2014-11-17 00";
	private Date flagDate;

	private final int pairFeatureNum = 4;
	//求和
	private final String[] keys={
			"clickTotal",
			"favTotal",
			"cartTotal",
			"buyTotal",
			
			"clickLastThreeday",
			"favLastThreeday",
			"cartLastThreeday",
			"buyLastThreeday",
			
			"clickLastWeekday",
			"favLastWeekday",
			"cartLastWeekday",
			"buyLastWeekday",
			
			"clickTheDayOfWeek",
			"favTheDayOfWeek",
			"cartTheDayOfWeek",
			"buyTheDayOfWeek"
	};
	private final String[] time=
		{
				"clickTime",
				"favTime",
				"cartTime",
				"buyTime"
		};
	
	//private long[] counts;
//	private Date[] maxTime;
//	private Date[]  minTime;
		
//	private Record pair;
	private Record result;
	@Override
	public void cleanup(TaskContext arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void reduce(Record key, Iterator<Record> values, TaskContext context)
			throws IOException {
		// TODO Auto-generated method stub
//		 pair.setBigint(0, key.getBigint(0));
//		 pair.setBigint(1, key.getBigint(1));
		
		 Date[] maxTime = new Date[pairFeatureNum];
		 Date[] minTime = new Date[pairFeatureNum];
		 long[] counts = new long[keys.length];
		 
		 
		 result.setBigint(0, key.getBigint(0));
		 result.setBigint(1, key.getBigint(1));
		 int curFeatureNum =2;
		 while (values.hasNext()) {
	            Record val = values.next();
	            for(int i = 0; i < this.keys.length; i++ )
	            {
	            	counts[i]= counts[i] +  val.getBigint(keys[i]);
	            }
	            //更新各个action的最大最小 和 全局的最大最小
	            for(int i = 0; i < pairFeatureNum;i++)
	            {
	            	//该key有值 
	            	if(!val.getDatetime(time[i]).equals(flagDate))
	            	{
	            		//maxtime 各个action
		            	if(maxTime[i] == null )
		            	{
		            		maxTime[i] = val.getDatetime(time[i]);
		            		
		            	}
		            	else
		            	{
		            		if(val.getDatetime(time[i]).after(maxTime[i]))
		            		{
		            			maxTime[i] = val.getDatetime(time[i]);
		            			
		            		}
		            	}
		            	
		            	//mintime 各个action的最小值
		            	if(minTime[i] == null)
		            	{
		            		minTime[i] = val.getDatetime(time[i]);
		            		
		            	}
		            	else
		            	{
		            		if(val.getDatetime(time[i]).before(minTime[i]))
	        				{
		            			minTime[i] = val.getDatetime(time[i]);
	        				}
		            	}
	            	}
	   	
	            }
	           
	     }
		 //前16维count 特征
		 for(int i = 0 ; i < keys.length;i++)
		 {
			 result.setBigint(i+curFeatureNum, counts[i]);	 
		 }
		 curFeatureNum += keys.length;
		 
		 //action perday
		 Calendar cal = Calendar.getInstance();
		 for(int i = 0; i< pairFeatureNum;i++)
		 {
			 double numPerday = 0;
			 //有该类别的行为
			 if(minTime[i] != null)
			 {
				 cal.setTime(minTime[i]);
				 int fday = cal.get(Calendar.DAY_OF_YEAR);
				 cal.setTime(maxTime[i]);
				 int lday = cal.get(Calendar.DAY_OF_YEAR);
				
				 if( (lday-fday) != 0)
				 {
					numPerday = (double)counts[i]/(double)(lday-fday);
				 }
				 else
				 {
					 numPerday = (double)counts[i];
				 }
				 
			 }
			 result.setDouble(i+curFeatureNum,numPerday );
		 }
		 curFeatureNum += pairFeatureNum;
		 //action flag
		 for(int i = 0; i< pairFeatureNum;i++)
		 {
			 if(counts[i] >0)
			 {
				 result.setBigint(i+curFeatureNum, 1L);
			 }
			 else
			 {
				 result.setBigint(i+curFeatureNum, 0L);
			 }
		 }
		 curFeatureNum += pairFeatureNum;
		 
		 //cvr
		 double cvr = 0;
		 if(counts[0] ==0)
		 {
			 cvr = (double)counts[3];
		 }
		 else
		 {
			 cvr = (double)counts[3]/(double)counts[0];
		 }
		 result.setDouble(curFeatureNum, cvr);
		 curFeatureNum += 1;
		 
		 //cvr perday
		 
		 //clickperday 的index
		 int actionPerdayIndex = 2+keys.length;
		 if(result.getDouble(actionPerdayIndex) == 0)
		 {
			 cvr = result.getDouble(actionPerdayIndex+3);
		 }
		 else
		 {
			 cvr = result.getDouble(actionPerdayIndex+3) / result.getDouble(actionPerdayIndex);
		 }
		 result.setDouble(curFeatureNum, cvr);
		 curFeatureNum += 1;
		 
		 //first day and last day of buy ,如果为空，设为默认时间
		 if(minTime[3] == null)
		 {
			 cal.setTime(defaultDate);
		 }
		 else
		 {
			 cal.setTime(minTime[3]);
		 }
		 int fday = cal.get(Calendar.DAY_OF_YEAR);
		 if(maxTime[3] == null)
		 {
			 cal.setTime(defaultDate);
		 }
		 else
		 {
			 cal.setTime(maxTime[3]);
		 }
		 int lday = cal.get(Calendar.DAY_OF_YEAR);
		 result.setBigint(curFeatureNum, (long) fday);
		 curFeatureNum++;
		 result.setBigint(curFeatureNum, (long) lday);
		 curFeatureNum++;
		 
		 //length of action(first day - last day)
		 long interval = 0;
	
		//所有action下的最小time和最大time 的index (比如，minTime[minTimeIndex] = 所有action下最小的时间)
		 int minIndex = minTimeIndex(minTime);
		 int maxIndex = maxTimeIndex(maxTime);
		 interval = hourDiff(minTime[minIndex], maxTime[maxIndex]);
		 result.setBigint(curFeatureNum,interval );
		 curFeatureNum ++;
		
		
		 //time interval from last click to end(last click 若没有 设为默认值)
		 interval = hourDiff(maxTime[0],splitPointDate );
		 result.setBigint(curFeatureNum,interval );
		 curFeatureNum ++;
		 
		 //time interval from last buy to end(last buy 若没有 设为默认值)	
		 interval = hourDiff( maxTime[3],splitPointDate);
		 result.setBigint(curFeatureNum,interval );
		 //curFeatureNum ++;
		//context.write(pair,result);
		context.write(result);
		
	}

	@Override
	public void setup(TaskContext context) throws IOException {
		// TODO Auto-generated method stub
//		pair = context.createOutputKeyRecord();
//		result=context.createOutputValueRecord();
		String timeSplitPointStr = context.getJobConf().get("seperator");
		result = context.createOutputRecord();
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh");
		try {
			splitPointDate = sdf.parse(timeSplitPointStr);
			defaultDate = sdf.parse(timeDefaultStr);
			flagDate = sdf.parse(timeFlagStr);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private long hourDiff(Date startTime ,Date endTime) 
	{
		if(startTime == null)
		{
			startTime = defaultDate;
		}
		if(endTime == null)
		{
			endTime = defaultDate;
		}
		 long diff;  
		 diff = endTime.getTime() - startTime.getTime();   
		 long hour = diff / (1000 * 60 * 60);
		 return hour;
	}
	private int minTimeIndex(Date[] d)
	{
		int min = -1;
		for (int i = 0; i < d.length; i++) {
			if(d[i] != null)
			{
				if(min == -1)
				{
					min= i;
				}
				else if(i != min  && d[i].before(d[min]))
				{
					min = i;
				}
			}
		}
		return min;
	}
	private int maxTimeIndex(Date[] d)
	{
		int max = -1;
		for (int i = 0; i < d.length; i++) {
			if(d[i] != null)
			{
				if(max == -1)
				{
					max= i;
				}
				else if(i != max  && d[i].after(d[max]))
				{
					max = i;
				}
			}
		}
		return max;
	}
}
