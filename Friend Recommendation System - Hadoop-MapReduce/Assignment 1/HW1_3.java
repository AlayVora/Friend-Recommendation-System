
/* 	Import Files */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HW1_3 extends Configured implements Tool {
	static String test = "";
	static HashMap<String, String> storeMap;

	static HashSet<Integer> temporaryMap = new HashSet<>();

	public static class FriendData extends Mapper<Text, Text, Text, Text> {

		String friendData;

		public void setup(Context context) throws IOException {
			Configuration config = context.getConfiguration();

			storeMap = new HashMap<String, String>();
			String mybusinessdataPath = config.get("businessdata");

			// Location of file in HDFS
			Path path = new Path("hdfs://cshadoop1" + mybusinessdataPath);
			FileSystem fs = FileSystem.get(config);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
			String line;
			line = br.readLine();
			while (line != null) {
				String[] myArr = line.split(",");
				if (myArr.length == 10) {
					String data = myArr[1] + ":" + myArr[6];
					storeMap.put(myArr[0].trim(), data);
				}
				line = br.readLine();
			}

		}

		// int count=0;

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] split = line.split(",");

			if (null != storeMap && !storeMap.isEmpty()) {
				for (String s : split) {
					if (storeMap.containsKey(s)) {
						friendData = storeMap.get(s);
						storeMap.remove(s);
						context.write(key, new Text(friendData));
					}
				}
			}
		}
	}

	public static class HW1_3Mapper extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			Text counter = new Text();
			String s = "";
			
			for (Text t : values) {
				if (s.equals(""))
					s = "[";
				s = s + t + ",";
			}
			
			s = s.substring(0, s.length() - 1);
			s = s + "]";
			counter.set(s);
			context.write(key, counter);
		}
	}

	public static void main(String args[]) throws Exception {
		int res = ToolRunner.run(new Configuration(), new HW1_3(), args);
		System.exit(res);

	}

	public int run(String[] otherArgs) throws Exception {
		Configuration conf = new Configuration();
		if (otherArgs.length != 6) {
			System.err.println("Usage: Error");
			System.exit(2);
		}

		conf.set("userA", otherArgs[0]);
		conf.set("userB", otherArgs[1]);

		Job job = new Job(conf, "HW1_2");
		job.setJarByClass(HW1_3.class);
		
		job.setMapperClass(MFriend.HW1_3_1Mapper.class);
		job.setReducerClass(MFriend.HW1_3_1Reducer.class);

		job.setOutputKeyClass(Text.class);

		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
		Path p = new Path(otherArgs[3]);
		FileOutputFormat.setOutputPath(job, p);

		int code = job.waitForCompletion(true) ? 0 : 1;

		Configuration conf1 = getConf();
		conf1.set("businessdata", otherArgs[4]);
		Job job2 = new Job(conf1, "sort");
		job2.setJarByClass(HW1_3.class);
		job2.setInputFormatClass(KeyValueTextInputFormat.class);

		job2.setMapperClass(FriendData.class);
		job2.setReducerClass(HW1_3Mapper.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job2, p);
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[5]));

		// Execute job and grab exit code
		code = job2.waitForCompletion(true) ? 0 : 1;

		FileSystem.get(conf).delete(p, true);
		System.exit(code);
		return code;
	}
}