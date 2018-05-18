package com.HadoopExample1;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PostalCodesMapper extends 
Mapper<LongWritable, Text, IntWritable, Text>  {

	@Override
	protected void map(LongWritable key, Text text, Context context) throws IOException,InterruptedException{
		String[] tokens = text.toString().split("\t");
		String total_tax_amount = tokens[8];
		String total_paid_amount = tokens[9];
		String billing_address_postal_code = tokens[24];
		context.write(new IntWritable(Integer.parseInt(billing_address_postal_code)), 
				new Text(total_tax_amount + "," +total_paid_amount));
		
	}
}
