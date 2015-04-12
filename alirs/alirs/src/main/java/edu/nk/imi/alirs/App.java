package edu.nk.imi.alirs;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	int fileNum = 200;
    	double lamda = 0.00001;
    	double rate = 100;
    	String pathPrefix = "/home/xuejiewu/文档/ali/data/online/";
    	String inputPrefix = pathPrefix+"candidate_feature/";
    	//String inputFile = "/home/xuejiewu/文档/ali/data/online/test_submit.txt";
    	String predictFile = pathPrefix+"predict_"+lamda+"_"+rate+".txt";
    	String modelFile =  pathPrefix+"model_submit_"+lamda+"_"+rate+".txt";
    	
    	String pairFile = pathPrefix+"test_pair.txt";
    	String resFile =  pathPrefix+"res_submit_"+lamda+"_"+rate+".txt";
    	 PredictLogistic pl = new PredictLogistic();
    	for (int i = 0; i < fileNum; i++) {
    		String inputFile = inputPrefix+"feature"+i+".txt";
    		try {
    			pl.predict(inputFile, modelFile, predictFile);
    		} catch (Exception e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    		}
    		
		}
    	DataTransform dt = new DataTransform();
    	try {
			dt.getFinalRes(pairFile, predictFile, resFile);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
    }
}
