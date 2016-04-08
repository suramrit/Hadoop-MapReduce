import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class WordCount {
public static class TokenizerMapper
extends Mapper<Object, Text, Text, IntWritable>{
//private final static IntWritable one = new IntWritable(1);
private Text word = new Text();


public void map(Object key, Text value, Context context
	) throws IOException, InterruptedException {
		//StringTokenizer itr = new StringTokenizer(value.toString());
		CharSequence uk = "Unknown";
		CharSequence ar = "Arr";
		boolean flag;
		int size = 0;
		String line  = value.toString();
		String[] data = line.split(Pattern.quote(","));
		
		try {
            size = Integer.parseInt(data[7]);
            flag = true;
			} catch (NumberFormatException nfe) {flag = false;}
        
		
		
		if(flag ==  true && size>=0 )
		{
			
		IntWritable count = new IntWritable(1);
		IntWritable count_zero = new IntWritable(0);

		String data_2 = data[2];
		String[] hall = data_2.split(("\\s++"));
			
		String temp = hall[0]+"_"+data[1];
		if(!hall[0].contains(uk) && !hall[0].contains(ar))
		{
		if(size ==0 )
		{	
			word.set(temp);
			context.write(word, count_zero);
		}
		else{
		while (size!=0) {
				size--;
				word.set(temp);
				context.write(word, count);

			}
		}	
		}
			
		}
}


}
public static class IntSumReducer
extends Reducer<Text,IntWritable,Text,IntWritable> {
private IntWritable result = new IntWritable();
public void reduce(Text key, Iterable<IntWritable> values,
Context context
) throws IOException, InterruptedException {
int sum = 0;
for (IntWritable val : values) {
sum += val.get();
}
result.set(sum);
context.write(key, result);
}
}
public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf, "WordCount");
	job.setJarByClass(WordCount.class);
	job.setMapperClass(TokenizerMapper.class);
	job.setCombinerClass(IntSumReducer.class);
	job.setReducerClass(IntSumReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}
