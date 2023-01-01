package com.yjx;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper需要修改参数
 * 需要重写Map方法
 */
public class MusicChartsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

}
