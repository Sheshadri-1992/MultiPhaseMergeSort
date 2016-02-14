package com.POD.iiit;

import java.lang.management.*;
import java.util.*;
import java.io.*;

public class MasterClass {

	
	public static HashMap<String,String> attr_type;
	public static HashMap<String,Integer> attr_pos;
	public static List<String> attr_sort_cols;
	public static List<String> attr_output_cols;
	public static long startTime;
	public static long endTime;
	public static long MAX_MEMORY;
	public static String order;
	public static void main(String[] args)
	{
		
		/**Declare your variables here**/
		startTime = System.currentTimeMillis();
		
		/**attribute type hash map **/		
		attr_type = new HashMap<String,String>();
		
		/**attribute position hash map **/
		attr_pos = new HashMap<String,Integer>();
		
		/**record length**/
		long rec_length=1;
		
		/**number of records**/
		long no_of_records_in_main_memory=1;
		
		/**total number of records in a given input file **/
		long total_records=1;
		
		/**Sorted lists**/
		double no_of_sorted_lists=1;
		
		/**Variable declarations end here**/
		
		System.out.println("POD Codemore welcomes you!");
		
		System.out.println("Metadata File ==> " + args[1]);
		System.out.println("Input File ==> " + args[3]);
		System.out.println("Output File ==> " + args[5]);
		System.out.println("Output columns ==> " + args[7]);
		System.out.println("Sort columns ==> " + args[9]);
		System.out.println("Order ==> " + args[11]);
		
		order=args[11];

		
		String line;
		/**reading metadata file**/
		FileReaderClass read_obj = new FileReaderClass(args[1]);
		read_obj.openFile();
		line = read_obj.readFile();
		int counter=0;
		while(line!=null)
		{
			String[] array = line.split(",");
			attr_type.put(array[0], array[1]);
			attr_pos.put(array[0], counter);
			counter++;
			line = read_obj.readFile();
		}
		read_obj.closeFile();
		/**end of reading**/

		System.out.println("Attribute type hashmap "+ attr_type.toString());
		System.out.println("Attribute position hashmap "+ attr_pos.toString());
		
		/**This is used to get the max memory alloted for the java**/
		getMaxMemory();
		
		
		rec_length=recordLength();
		no_of_records_in_main_memory = getNoOfSortedRecords(rec_length);
		total_records = noOfRecordsInGivenInput(args[3]);//this is the input file
		no_of_sorted_lists = Math.ceil(noOfSortedSublists(total_records,no_of_records_in_main_memory));
		
		System.out.println("Record length is = " + rec_length);
		System.out.println("No of records in main memory = "+ no_of_records_in_main_memory);
		System.out.println("Total records are = "+ total_records);
		System.out.println("Number of sorted sub lists are = "+no_of_sorted_lists);
		
		
		/**we have number of sorted sublists now, we have to print whether we can sort it or not **/
		boolean ext_merge = extMergePossible(no_of_sorted_lists, rec_length); 
		if(ext_merge)
		{
			System.out.println("Multiway Merge is possible");
			
			attr_sort_cols = new ArrayList<String>();
			attr_output_cols = new ArrayList<String>();
			
			//do phase 1 and phase 2 here
			String[] sort_columns = args[9].split(",");
			for(int i=0;i<sort_columns.length;i++)
			{
				
				attr_sort_cols.add(i,sort_columns[i]);
			}
			
			String[] output_columns = args[7].split(",");
			for(int i=0;i<output_columns.length;i++)
			{
				attr_output_cols.add(i,output_columns[i]);
			}
			
			System.out.println("The sort columns are " + attr_sort_cols.toString());
			System.out.println("Max memory is "+MAX_MEMORY);
			
			int sort_order=0;
			if(order.contains("asc"))
				sort_order=0;
			else
				sort_order=1;
			//First Phase Merge sort
			FirstPhaseSort myFirst = new FirstPhaseSort(attr_type, attr_pos, attr_sort_cols, args[3], total_records, no_of_sorted_lists,no_of_records_in_main_memory,sort_order);
			myFirst.collectRecords();
			
			
			
			
			System.out.println("The sort order is "+sort_order);
			//Second phase of merge sort
			double blockSize =computeBlockSize(no_of_sorted_lists, rec_length);
			SecondPhaseSort mySecond = new SecondPhaseSort(blockSize, attr_type, attr_pos, attr_sort_cols,attr_output_cols, "inter_part", no_of_sorted_lists);
			mySecond.createOutputFile();
			mySecond.openNFiles();
			mySecond.fillInitialBlocks();
			mySecond.mergeLists(sort_order);
			mySecond.closeNFiles();
//			System.out.println("Block size is " + Math.floor(blockSize));
		}
		else
		{
			System.out.println("Multiway merge is not possible");
		}
		endTime=System.currentTimeMillis();
		
		long totaltime=endTime-startTime;
		double mins = (double)totaltime/(1000*60);
		System.out.println("Total time taken is "+totaltime+"ms "+" And "+mins+"mins");
		
	}
	
	/**method used to find number of records in a block **/
	public static double computeBlockSize(double numberOfSublists,double recordLength)
	{
		return MAX_MEMORY/((numberOfSublists+1)*recordLength);
	}

	
	/**Method used to get the length of records **/
	public static long recordLength()
	{	
		long record_length=0;
		
		for(String key: attr_type.keySet())
		{
			String dataType = attr_type.get(key);
			if(dataType.contains("int")||dataType.contains("INT"))
			{
				record_length+=8;//specified 6 digits in given example (keep 8)
			}
			
			if(dataType.contains("date")||dataType.contains("DATE"))
			{
				record_length+=12;//10 but keep 2 extra
			}
			
			if(dataType.contains("char") || dataType.contains("CHAR"))
			{
				String charLength = dataType.substring(dataType.indexOf("(")+1,dataType.indexOf(")"));
				Integer charLength_in_Integer = Integer.parseInt(charLength);
				System.out.println("The size of char is " + charLength_in_Integer);
				charLength_in_Integer = charLength_in_Integer +2; //for NULL character
				record_length+= charLength_in_Integer;
			}
			
		}
		
		/**since each character in java is encoded by UTF 8 it takes 2 bytes 
		 * Also string object holds 24 bytes for char array, offset and count
		 * 
		 * Total record size = 24bytes(fixed) + record_length*2;
		 * 
		 * **/
		
		record_length = record_length*2; //record_length = 24 + record_length*2;  
		
		return record_length;
	}
	
	/**Number of records present in the given input file **/
	public static long noOfRecordsInGivenInput(String filename)
	{
		long counter=0;
		
		FileReaderClass myReader = new FileReaderClass(filename);
		myReader.openFile();
		try
		{
			String line = myReader.buff_reader.readLine();
			
			while(line!=null)
			{
				counter++;
				line =  myReader.buff_reader.readLine();
			}
		}
		catch(IOException e)
		{
			System.out.println("IO excpetio in method noOfRecors class: main ");
		}
		
		myReader.closeFile();
		
		return counter;
						
	}
	
	/**Number of records that can be sorted in main memory **/
	public static long getNoOfSortedRecords(long rec_length)	
	{
		if(rec_length==0)
		{
			System.out.println("check rec length again, it is 0 here");
			return 1;
		}
		return MAX_MEMORY/rec_length;
	}
	
	
	/** No of sorted sublists that can be created **/
	public static double noOfSortedSublists(double totalrecords,double rec_main_memory)
	{
		return totalrecords/rec_main_memory;
	}
	
	public static void getMaxMemory()
	{
		MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
		MAX_MEMORY = memoryBean.getHeapMemoryUsage().getMax();
//		5 MB is subtracted so that all local variables, references can be supported
		MAX_MEMORY = (MAX_MEMORY/2) - (5242880);
	}
	
	public static boolean extMergePossible(double noOfSortedSublists,long recordLength)
	{
		double result = noOfSortedSublists*recordLength + recordLength; //one for the output buffer
		
		if(result>= MAX_MEMORY)
			return false;
		else
			return true;
	}
	
}