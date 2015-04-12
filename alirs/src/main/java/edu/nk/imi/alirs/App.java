package edu.nk.imi.alirs;

import edu.nk.imi.alirs.process.MergeAndPredictLR;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
       int rate = 100;
       double lamda = 0.00001;
       int topN = 10;
       String pathPrefix = "E:\\百度云同步盘\\研究生\\研究\\项目\\aliRS2015\\data\\train&test\\4_11\\";
       String pairPath = pathPrefix+"feature.txt";
       String userPath = pathPrefix+"tmp_user.txt";
       String itemPath = pathPrefix+"tmp_item_filter.txt";
       String testPairPath = "E:\\百度云同步盘\\研究生\\研究\\项目\\aliRS2015\\data\\train&test\\4_10\\"+"test_pair.txt";
       //String testPairPath = pathPrefix+"test_pair.txt";
       //LR 专用
       String modelPath = pathPrefix+"model_submit_"+lamda+"_"+rate+".txt";
       String resPath = pathPrefix+"res_"+lamda+"_"+rate+".txt";
       MergeAndPredictLR mplr = new MergeAndPredictLR();
       mplr.mergeFeatureAndPredict(pairPath, testPairPath, itemPath, userPath, modelPath, resPath,topN);
    }
}
