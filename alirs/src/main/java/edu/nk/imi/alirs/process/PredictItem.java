package edu.nk.imi.alirs.process;

public class PredictItem {
	String itemId;
	double score;
	public PredictItem(String itemId, double score)
	{
		this.itemId  = itemId;
		this.score = score;
	}
	public String getItemId() {
		return itemId;
	}
	public void setItemId(String itemId) {
		this.itemId = itemId;
	}
	public double getScore() {
		return score;
	}
	public void setScore(double score) {
		this.score = score;
	}
	

}
