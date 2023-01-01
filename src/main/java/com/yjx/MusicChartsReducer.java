package com.yjx;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer参数需要修改
 * 需要重写Reduce方法
 */
public class MusicChartsReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

}
