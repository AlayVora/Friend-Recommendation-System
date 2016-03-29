import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HW1_4 {
	static int i = 0;

	public static class HW1_4Mappaer extends Mapper<LongWritable, Text, LongWritable, Text> {
		LongWritable user = new LongWritable();
		Text friend = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line[] = value.toString().split("\t");
			user.set(Long.parseLong(line[0]));
			Text data = new Text();
			data.set(user.toString());

			if (line.length != 1) {
				String mFriends = line[1];
				String outvalue = ("U:" + mFriends.toString());
				context.write(user, new Text(outvalue));
			}
		}
	}

	public static class HW1_4_1Mapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		private LongWritable outkey = new LongWritable();
		private Text outvalue = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String arr[] = value.toString().split(",");
			if (arr.length == 10) {

				outkey.set(Long.parseLong(arr[0]));
				String[] cal = arr[9].toString().split("/");
				System.out.println("Ratings");
				Date now = new Date();
				int nowMonth = now.getMonth() + 1;
				int nowYear = now.getYear() + 1900;
				int result = nowYear - Integer.parseInt(cal[2]);

				if (Integer.parseInt(cal[0]) > nowMonth) {
					result--;
				} else if (Integer.parseInt(cal[0]) == nowMonth) {
					int nowDay = now.getDate();

					if (Integer.parseInt(cal[1]) > nowDay) {
						result--;
					}
				}
				String data = arr[1] + "," + new Integer(result).toString() + "," + arr[3] + "," + arr[4] + "," + arr[5];
				outvalue.set("R:" + data);
				context.write(outkey, outvalue);
			}
		}
	}
	
	public static class HW1_4Reducer extends Reducer<LongWritable, Text, Text, Text> {
		private ArrayList<Text> listA = new ArrayList<Text>();
		private ArrayList<Text> listB = new ArrayList<Text>();
		HashMap<String, String> myMap = new HashMap<>();

		public void setup(Context context) throws IOException {
			Configuration config = context.getConfiguration();
			myMap = new HashMap<String, String>();
			String mybusinessdataPath = config.get("businessdata");

			Path pt = new Path("hdfs://cshadoop1" + mybusinessdataPath);
			FileSystem fs = FileSystem.get(config);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
			String line;
			line = br.readLine();
			while (line != null) {
				String[] arr = line.split(",");
				if (arr.length == 10) {
					myMap.put(arr[0].trim(), arr[1] + ":" + arr[3] + ":" + arr[9]);
				}
				line = br.readLine();
			}
		}

		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			listA.clear();
			listB.clear();

			for (Text value : values) {
				if (value.toString().charAt(0) == 'U') {
					listA.add(new Text(value.toString().substring(2)));
				} else if (value.toString().charAt(0) == 'R') {
					listB.add(new Text(value.toString().substring(2)));
				}
			} 
			
			Text C = new Text();
			float age = 0;
			int count = 0;
			float averageAge;
			String[] details = null;
			
			if (!listA.isEmpty() && !listB.isEmpty()) {
				for (Text A : listA) {
					String frd[] = A.toString().split(",");

					for (int i = 0; i < frd.length; i++) {
						if (myMap.containsKey(frd[i])) {
							String[] ageCalu = myMap.get(frd[i]).split(":");
							Date now = new Date();
							int Month = now.getMonth() + 1;
							int Year = now.getYear() + 1900;
							String[] cal = ageCalu[2].toString().split("/");
							int result = Year - Integer.parseInt(cal[2]);

							if (Integer.parseInt(cal[0]) > Month) {
								result--;
							} else if (Integer.parseInt(cal[0]) == Month) {
								int Day = now.getDate();

								if (Integer.parseInt(cal[1]) > Day) {
									result--;
								}
							}
							age += result;
							count++;
						}
					}
					averageAge = (float) (age / count);
					String S = "";

					for (Text B : listB) {
						details = B.toString().split(",");
						S = B.toString() + "," + new Text(new FloatWritable((float) averageAge).toString());
					}

					C.set(S);
				}

			}
			context.write(new Text(key.toString()), C);
		}

	}

	public static class HW1_4_2Mapper extends Mapper<LongWritable, Text, UserPageWritable, Text> {
		private Long outkey = new Long(0L);
		

		public Long getOutkey() {
			return outkey;
		}

		public void setOutkey(Long outkey) {
			this.outkey = outkey;
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] m = value.toString().split("\t");
			Long l = Long.parseLong(m[0]);
			outkey = l;
			if (m.length == 2) {
				String line[] = m[1].split(",");

				context.write(new UserPageWritable(Float.parseFloat(m[0]), Float.parseFloat(line[5])),
						new Text(m[1].toString()));
			}
		}

	}

	public static class UserPageWritable implements WritableComparable<UserPageWritable> {

		private Float userId;
		private Float friendId;

		public Float getUserId() {
			return userId;
		}

		public void setUserId(Float userId) {
			this.userId = userId;
		}

		public Float getFriendId() {
			return friendId;
		}

		public void setFriendId(Float friendId) {
			this.friendId = friendId;
		}

		public UserPageWritable(Float user, Float friend1) {
			// TODO Auto-generated constructor stub
			this.userId = user;
			this.friendId = friend1;
		}

		public UserPageWritable() {
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			userId = in.readFloat();
			friendId = in.readFloat();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeFloat(userId);
			;
			out.writeFloat(friendId);
			;
		}

		@Override
		public int compareTo(UserPageWritable o) {
			// TODO Auto-generated method stub

			int result = userId.compareTo(o.userId);
			if (result != 0) {
				return result;
			}
			return this.friendId.compareTo(o.friendId);
		}

		@Override
		public String toString() {
			return userId.toString() + ":" + friendId.toString();
		}

		@Override
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

	public class TemperaturePartitioner extends Partitioner<UserPageWritable, Text> {
		public int getPartition(UserPageWritable temperaturePair, Text nullWritable, int numPartitions) {
			return temperaturePair.getFriendId().hashCode() % numPartitions;
		}
	}

	public static class SecondarySortBasicCompKeySortComparator extends WritableComparator {

		public SecondarySortBasicCompKeySortComparator() {
			super(UserPageWritable.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			UserPageWritable key1 = (UserPageWritable) w1;
			UserPageWritable key2 = (UserPageWritable) w2;

			int cmpResult = -1 * key1.getFriendId().compareTo(key2.getFriendId());

			return cmpResult;
		}
	}

	public static class SecondarySortBasicGroupingComparator extends WritableComparator {
		public SecondarySortBasicGroupingComparator() {
			super(UserPageWritable.class, true);
		}

		
		public int compare(WritableComparable w1, WritableComparable w2) {
			UserPageWritable key1 = (UserPageWritable) w1;
			UserPageWritable key2 = (UserPageWritable) w2;
			return -1 * key1.getFriendId().compareTo(key2.getFriendId());
		}
	}

	public static class UserRatingsMoviesReducer extends Reducer<UserPageWritable, Text, Text, Text> {
		private ArrayList<Text> listA = new ArrayList<Text>();
		private ArrayList<Text> listB = new ArrayList<Text>();
		int i = 0;
		TreeMap<String, String> hass = new TreeMap<String, String>();

		public void reduce(UserPageWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			

			for (Text t : values) {
				
				if (hass.size() < 20) {
					hass.put(key.userId.toString(), t.toString());
					context.write(new Text(t.toString().split(",")[0]), new Text(t));
				}
			}
		}
	}

	// Driver code
	public static void main(String[] args) throws Exception {

		Path outputDirIntermediate1 = new Path(args[3] + "_int1");
		Path outputDirIntermediate2 = new Path(args[4] + "_int2");

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("businessdata", otherArgs[0]);
		// get all args
		if (otherArgs.length != 5) {
			System.err.println("Usage: JoinExample <in> <in2> <in3> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "join1 ");
		job.setJarByClass(HW1_4.class);
		job.setReducerClass(HW1_4Reducer.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, HW1_4Mappaer.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[2]), TextInputFormat.class, HW1_4_1Mapper.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		
		FileOutputFormat.setOutputPath(job, outputDirIntermediate1);

		int code = job.waitForCompletion(true) ? 0 : 1;
		Job job1 = new Job(new Configuration(), "join2");
		job1.setJarByClass(HW1_4.class);

		
		FileInputFormat.addInputPath(job1, new Path(args[3] + "_int1"));

		job1.setMapOutputKeyClass(UserPageWritable.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setPartitionerClass(TemperaturePartitioner.class);
		job1.setMapperClass(HW1_4_2Mapper.class);
		
		job1.setSortComparatorClass(SecondarySortBasicCompKeySortComparator.class);
		job1.setGroupingComparatorClass(SecondarySortBasicGroupingComparator.class);
		job1.setReducerClass(UserRatingsMoviesReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job1, outputDirIntermediate2);

		// Execute job and grab exit code
		code = job1.waitForCompletion(true) ? 0 : 1;

	}
}