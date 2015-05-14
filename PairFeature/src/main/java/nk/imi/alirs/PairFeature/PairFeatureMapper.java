package nk.imi.alirs.PairFeature;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Mapper;

public class PairFeatureMapper implements Mapper{

	private final String[] total={
			"clickTotal",
			"favTotal",
			"cartTotal",
			"buyTotal"
	};
	private final String[] lastThreeDay=
		{
				"clickLastThreeday",
				"favLastThreeday",
				"cartLastThreeday",
				"buyLastThreeday"
		};
	private final String[] lastWeekDay={
			"clickLastWeekday",
			"favLastWeekday",
			"cartLastWeekday",
			"buyLastWeekday"
	};
	
	private final String[] theDayOfWeek = 
	{
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
//	private final String clickTotal = "clickTotal";
//	private final String favTotal = "favTotal";
//	private final String cartTotal = "cartTotal";
//	private final String buyTotal = "buyTotal";
//	
//	private final String clickLastThreeDay = "clickLastThreeday";
//	private final String favLastThreeDay = "favLastThreeday";
//	private final String cartLastThreeDay = "cartLastThreeday";
//	private final String buyLastThreeDay = "buyLastThreeday";
//	
//	private final String clickLastWeekDay = "clickLastWeekday";
//	private final String favLastWeekDay = "favLastWeekday";
//	private final String cartLastWeekDay = "cartLastWeekday";
//	private final String buyLastWeekDay = "buyLastWeekday";
//	
//	private final String clickTheDayOfWeek = "clickTheDayOfWeek";
//	private final String favTheDayOfWeek = "favTheDayOfWeek";
//	private final String cartTheDayOfWeek = "cartTheDayOfWeek";
//	private final String buyTheDayOfWeek = "buyTheDayOfWeek";
	
//	private final String clickTime = "clickTime";
//	private final String favTime = "favTime";
//	private final String cartTime = "cartTime";
//	private final String buyTime = "buyTime";
	
//	private final String timeSplitPointStr = "2014-12-19 00";
	//不可能出现的日期
	private final String timeFlagStr = "2014-11-17 00";

	private Date splitPointDate ;
	private Date lastThreeDayDate;
	private Date lastWeekDate;
	private Date flagDate;
	
	//type= 0 pair ; type=1 cpair
	private int type ;
	//分割点日期的 day of week
	private int dayofweek;
	//pair： user_id ,item_id
	private Record pair;
	//feature: totalNum 【action】
	private Record feature;
	@Override
	public void cleanup(TaskContext context) throws IOException {
		// TODO Auto-generated method stub
		
	}

	//record: userid ,itemid,behavior_type, user_geohash,item_category,time
	@Override
	public void map(long recordNum, Record record, TaskContext context)
			throws IOException {
		// TODO Auto-generated method stub
		String dateStr = record.getString(5);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh");
		Date d = null;
		try {
			d=sdf.parse(dateStr);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//该记录在分割点之前
		if(d.before(splitPointDate))
		{
			
			pair.setBigint(0, record.getBigint(0));
			if(type == 0)
			{
				pair.setBigint(1, record.getBigint(1));
			}
			else
			{
				pair.setBigint(1, record.getBigint(4));
			}
			long behaviorType = record.getBigint(2);
			//初始化
			initFeature(feature);
			feature.setBigint(total[(int) (behaviorType-1L)], 1L);
			
			

			//是否一周之内的
			if( d.after(lastWeekDate) || d.equals(lastWeekDate))
			{
				feature.setBigint(lastWeekDay[(int) (behaviorType-1L)], 1L);
				//是否在三天之内
				if(d.after(lastThreeDayDate) || d.equals(lastThreeDayDate))
				{
					feature.setBigint(lastThreeDay[(int) (behaviorType-1L)], 1L);
				}
			}
			//是否是相等的day of week
			Calendar cal = Calendar.getInstance();
			cal.setTime(d);
			int dayofweekTmp = cal.get(Calendar.DAY_OF_WEEK);
			if( dayofweekTmp == dayofweek) 
			{
				feature.setBigint(theDayOfWeek[(int) (behaviorType-1L)], 1L);
			}
			feature.setDatetime(time[(int) (behaviorType-1L)], d);
			context.write(pair, feature);
			
		}
		
	}

	@Override
	public void setup(TaskContext context) throws IOException {
		// TODO Auto-generated method stub
		String timeSplitPointStr = context.getJobConf().get("seperator");
        type = Integer.parseInt(context.getJobConf().get("type"));

		pair = context.createMapOutputKeyRecord();
		feature = context.createMapOutputValueRecord();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh");
		try {
			splitPointDate = sdf.parse(timeSplitPointStr);
			flagDate = sdf.parse(timeFlagStr);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Calendar cal = Calendar.getInstance();
		cal.setTime(splitPointDate);
		//分割点日期的 day of week
		dayofweek = cal.get( Calendar.DAY_OF_WEEK );
		
		//最近三天的基准
		cal.add(Calendar.DAY_OF_MONTH, -3);
		lastThreeDayDate = cal.getTime();
		
		//最近一周的基准
		cal.clear();
		cal.setTime(splitPointDate);
		
		cal.add(Calendar.DAY_OF_MONTH, -7);
		lastWeekDate = cal.getTime();
	}

	private void initFeature(Record feature)
	{
		for(int i = 0; i < 4 ; i++)
		{
			feature.setBigint(total[i], 0L);
			feature.setBigint(lastThreeDay[i], 0L);
			feature.setBigint(lastWeekDay[i],0L);
			feature.setBigint(theDayOfWeek[i],0L);
			feature.setDatetime(time[i], flagDate);
			//feature.setDatetime(time[i], );
		}
	}
}
