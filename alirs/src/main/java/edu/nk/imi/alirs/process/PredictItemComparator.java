package edu.nk.imi.alirs.process;

import java.util.Comparator;


public class PredictItemComparator implements Comparator<PredictItem>
{

	public int compare(PredictItem o1, PredictItem o2) {
		// TODO Auto-generated method stub
		if ( o1.getScore() > o2.getScore())
		{
				return 1;
		}
		else if (o1.getScore() > o2.getScore())
		{
			return -1;
		}
		else return 0;
	}
}