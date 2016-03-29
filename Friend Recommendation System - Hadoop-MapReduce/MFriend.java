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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MFriend extends Configured implements Tool {

	public static class HW1_3_1Mapper extends Mapper<LongWritable, Text, Text, Text> {

		Long temp = new Long(-1L);

		Long fUser = new Long(-1L);
		Long sUser = new Long(-1L);
		Long inputUsr = new Long(-1L);

		String u1 = "", u2 = "";

		public void setup(Context context) {
			Configuration config = context.getConfiguration();
			u1 = config.get("userA");
			u2 = config.get("userB");
			fUser = Long.parseLong(u1);
			sUser = Long.parseLong(u2);
		}

		// int count=0;
		// private Text m_id = new Text();
		private Text m_others = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] split = line.split("\t");

			final Logger log = Logger.global;
			String subject = split[0];
			inputUsr = Long.parseLong(subject);
			
			if (split.length == 2) {
				String others = split[1];

				if ((inputUsr.equals(fUser)) || (inputUsr.equals(sUser))) {
					m_others.set(others);
					if (inputUsr.equals(fUser))
						temp = sUser;
					else
						temp = fUser;
					
					UserPageWritable data = null;
					String sol = "";
					if (inputUsr.compareTo(temp) < 0) {

						data = new UserPageWritable(inputUsr, temp);
						sol = data.toString();
						context.write(new Text(sol), m_others);

					} else {
						data = new UserPageWritable(temp, inputUsr);
						sol = data.toString();
						context.write(new Text(sol), m_others);
					}
				}

			}
		}
	}

	public static class HW1_3_1Reducer extends Reducer<Text, Text, Text, Text> {
		HashMap<String, Integer> hash = new HashMap<String, Integer>();

		// Intersection
		private HashSet<Integer> intersection(String s1, String s2) {

			HashSet<Integer> hash1 = new HashSet<Integer>();
			HashSet<Integer> hash2 = new HashSet<Integer>();
			if (null != s1) {
				String[] s = s1.split(",");
				for (int i = 0; i < s.length; i++) {
					hash1.add(Integer.parseInt(s[i]));
				}
			}

			if (null != s2) {
				String[] sa = s2.split(",");
				for (int i = 0; i < sa.length; i++) {
					if (hash1.contains(Integer.parseInt(sa[i]))) {
						hash2.add(Integer.parseInt(sa[i]));
					}
				}
			}

			return hash2;

		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			String[] combined = new String[2];
			int cur = 0;
			for (Text value : values) {
				combined[cur++] = value.toString();
			}

			if (null != combined[0]) {
				combined[0] = combined[0].replaceAll("[^0-9,]", "");

			}
			if (null != combined[1]) {
				combined[1] = combined[1].replaceAll("[^0-9,]", "");
			}

			HashSet<Integer> ca = intersection(combined[0], combined[1]);

			String answer = StringUtils.join(",", ca);
			String data = key.toString() + ":" + answer;
			context.write(new Text(key.toString()), new Text(StringUtils.join(",", ca)));

		}
	}

	public static class UserPageWritable implements WritableComparable<UserPageWritable> {

		private Long userId, friendId;

		public UserPageWritable(Long user, Long friend1) {
			this.userId = user;
			this.friendId = friend1;
		}

		public UserPageWritable() {
		}

		public void readFields(DataInput in) throws IOException {
			userId = in.readLong();
			friendId = in.readLong();
		}

		public void write(DataOutput out) throws IOException {
			out.writeLong(userId);
			out.writeLong(friendId);
		}

		public int compareTo(UserPageWritable o) {
			int result = userId.compareTo(o.userId);
			if (result != 0) {
				return result;
			}
			return this.friendId.compareTo(o.friendId);
		}

		public String toString() {
			return userId.toString() + "  " + friendId.toString();
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
		int res = ToolRunner.run(new Configuration(), new HW1_2(), args);
		System.exit(res);

	}

	public int run(String[] otherArgs) throws Exception {
		Configuration conf = new Configuration();
		if (otherArgs.length != 4) {
			System.err.println("Usage: UserRatedStanford <inbusiness> <inbusiness> <review> <out>");
			System.exit(2);
		}

		conf.set("userA", otherArgs[0]);
		conf.set("userB", otherArgs[1]);

		Job job = new Job(conf, "HW1_2");

		job.setJarByClass(MFriend.class);

		job.setMapperClass(HW1_3_1Mapper.class);
		job.setReducerClass(HW1_3_1Reducer.class);

		job.setOutputKeyClass(Text.class);

		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[2]));

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

		boolean sucess = job.waitForCompletion(true);
		return (sucess ? 0 : 1);
		// return 0;
	}
}