package edu.nk.imi.alirs.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class UitlReader {

public BufferedReader reader = null;
	
	String path = "";

	public void init(String p_path)
	{
		try {
			path = p_path;
			reader = new BufferedReader(new FileReader(new File(path)));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		};
	}

	public String nextLine()
	{
		try {
			return reader.readLine();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public void closeReader()
	{
		try {
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void reset()
	{
		try {
			reader.close();
			reader = new BufferedReader(new FileReader(new File(path)));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	// read specific line
    public String readAppointedLineNumber(int lineNumber)
    {  
    	try {
    		reset();
            String line = "";
            int count = 0;  
            while (line != null)
            {  
            	count++;  
            	line = reader.readLine();  
                if(count == lineNumber) {  
                	break;
                }  
            }
            reset();
            return line;
    	}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	return null;
    }  
}
