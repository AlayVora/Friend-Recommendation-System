/*Import Files*/
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.logging.Logger;

/* Import Apache Hadoop Files*/
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class HW1_2 extends Configured implements Tool{

	/* Mapper Class*/
	public static class HW1_2Mapper extends Mapper<LongWritable, Text, UserPageWritable, Text> {
		LongWritable User_A = new LongWritable();
		LongWritable User_B = new LongWritable();
		LongWritable User = new LongWritable();
		
		Long tempVar = new Long(-1L);
		Long ONE_A = new Long(-1L);
		Long ONE_B = new Long(-1L);
		Long THREE_C = new Long(-1L);
		
		String usr_1 = "", usr_2 = "";
		
		public void setup(Context context) {
			Configuration config = context.getConfiguration();
			usr_1 = config.get("userA");
			usr_2 = config.get("userB");
			
			ONE_A = Long.parseLong(usr_1);
			ONE_B = Long.parseLong(usr_2);
		}
		
		//int counterVar = 0;
		
		private Text mutualOthers = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String lines = value.toString();
			String[] splitLines = lines.split("\t");

			final Logger log = Logger.global;
			String subject = splitLines[0];
			THREE_C = Long.parseLong(subject);
			if(splitLines.length==2)
			{
				String others = splitLines[1];

				log.info("Looping" + User);
				if((THREE_C.equals(ONE_A)) || (THREE_C.equals(ONE_B)))
				{
					mutualOthers.set(others);
					if(THREE_C.equals(ONE_A))
						tempVar = ONE_B;
					else
						tempVar = ONE_A;
					if(THREE_C.compareTo(tempVar) < 0 )
					{
						context.write(new UserPageWritable(THREE_C,tempVar), mutualOthers );

					}
					else
					{
						context.write(new UserPageWritable(tempVar,THREE_C), mutualOthers );
					}             
				}

			}
		}
	}


	public static class HW1_2Reducer extends Reducer<UserPageWritable, Text, UserPageWritable, Text> {
		
		HashMap<String, Integer> hash = new HashMap<String, Integer>();
		private HashSet<Integer> intersection(String str1, String str2) {

			HashSet<Integer> hash1 = new HashSet<Integer>();
			HashSet<Integer> hash2 = new HashSet<Integer>();
			String[] stringArray = str1.split(",");
			for(int i=0; i<stringArray.length; i++)
			{ 
				hash1.add(Integer.parseInt(stringArray[i]));
			}


			if(null!= str2)
			{
				String[] stringArray2 = str2.split(",");
				for(int i=0;i<stringArray2.length;i++)
				{
					if(hash1.contains(Integer.parseInt(stringArray2[i])))
					{
						hash2.add(Integer.parseInt(stringArray2[i]));
					}
				}
			}

			return hash2;


		}

		public void reduce(UserPageWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			String[] combinedData = new String[2];
			int current = 0;
			for(Text value : values) {
				combinedData[current++] = value.toString();
			}
			combinedData[0] = combinedData[0].replaceAll("[^0-9,]", "");
			if(null!=combinedData[1])
				combinedData[1] = combinedData[1].replaceAll("[^0-9,]", "");

			HashSet<Integer> ca = intersection(combinedData[0], combinedData[1]);

			context.write(key, new Text(StringUtils.join(",", ca)));

		}
	}
	
	public static class UserPageWritable implements  WritableComparable<UserPageWritable>  {

		private Long userId, friendId;

		public UserPageWritable(Long user, Long friend1) {
			this.userId = user;
			this.friendId = friend1;
		}
		
		public UserPageWritable(){}
		
		public void readFields(DataInput in) throws IOException {
			userId = in.readLong();
			friendId = in.readLong();
		}

		public void write(DataOutput out) throws IOException {
			out.writeLong(userId);
			out.writeLong(friendId);
		}

		
		public String toString() {
			return userId.toString() + "," + friendId.toString();
		}
		
		public int compareTo(UserPageWritable o) {
			int result = userId.compareTo(o.userId);
			if (result != 0) {
				return result;
			}
			return this.friendId.compareTo(o.friendId);
		}
		
		public boolean equals(Object obj) {
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			final UserPageWritable other = (UserPageWritable) obj;
			if (this.userId != other.userId && (this.userId == null || !this.userId.equals(other.userId))) {
				return false;
			}
			if (this.friendId != other.friendId && (this.friendId == null || !this.friendId.equals(other.friendId))) {
				return false;
			}
			return true;
		}

	}
	public static void main(String args[]) throws Exception {
		int toolRunner = ToolRunner.run(new Configuration(), new HW1_2(), args);
		System.exit(toolRunner);

	}
	
	public int run(String[] otherArgs) throws Exception {
		Configuration conf = new Configuration();
		if (otherArgs.length != 4) {
			System.err.println("Usage: Error Message");
			System.exit(2);
		}

		conf.set("userA", otherArgs[0]);
		conf.set("userB", otherArgs[1]);

		Job job = new Job(conf, "HW1_2");
		job.setJarByClass(HW1_2.class);


		job.setMapperClass(HW1_2Mapper.class);
		job.setReducerClass(HW1_2Reducer.class);
		job.setOutputKeyClass(UserPageWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

		boolean sucess = job.waitForCompletion(true);
		return (sucess ? 0 : 1);
	}
}