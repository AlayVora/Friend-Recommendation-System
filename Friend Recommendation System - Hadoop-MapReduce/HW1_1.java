/* Import Files */ 
import java.util.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*** Apache Hadoop Import Files  ***/
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;



public class HW1_1 {
	static public class friendCounts implements Writable {
		public Long userId, mutualFriends;

		public void readFields(DataInput in) throws IOException {
			userId = in.readLong();
			mutualFriends = in.readLong();
		}
		
		public void write(DataOutput out) throws IOException {
			out.writeLong(userId);
			out.writeLong(mutualFriends);
		}
		
		public friendCounts(Long userId, Long mutualFriends) {
			this.userId = userId;
			this.mutualFriends = mutualFriends;
		}
		
		public friendCounts() {
			this(-1L, -1L);
		}
		
		public String toString() {
			return " toUser: "
					+ Long.toString(userId) + " Mutual Friends: " + Long.toString(mutualFriends);
		}
	}

	
	//Mapper Class
	public static class HW1_1Mapper extends Mapper<LongWritable, Text, LongWritable, friendCounts> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line[] = value.toString().split("\t");
			Long f_user = Long.parseLong(line[0]); 					//from User
			List<Long> to_user = new ArrayList<Long>(); 			//to user
			if (line.length == 2) {
				StringTokenizer token = new StringTokenizer(line[1], ",");
				while (token.hasMoreTokens()) {
					Long toUser = Long.parseLong(token.nextToken());
					to_user.add(toUser);
					context.write(new LongWritable(f_user), new friendCounts(toUser, -1L));
				}
				
				
				for (int a = 0; a < to_user.size(); a++) {
					for (int b = a + 1; b < to_user.size(); b++) {
						context.write(new LongWritable(to_user.get(a)), new friendCounts((to_user.get(b)), f_user));
						context.write(new LongWritable(to_user.get(b)), new friendCounts((to_user.get(a)), f_user));
					}
				}
			}
		}
	}
	
	//Reducer Class
	/*Key-> Recommended Friend, Value-> List of Mutual Friend*/
	public static class HW1_1Reducer extends Reducer<LongWritable, friendCounts, LongWritable, Text> {
		public void reduce(LongWritable key, Iterable<friendCounts> values, Context context)
				throws IOException, InterruptedException {
			final java.util.Map<Long, List<Long>> mutualFriends = new HashMap<Long, List<Long>>();
			for (friendCounts value : values) {
				final Boolean isFriend = (value.mutualFriends == -1);
				final Long toUser = value.userId;
				final Long mutualFriend = value.mutualFriends;

				if (mutualFriends.containsKey(toUser)) {
					if (isFriend) {
						mutualFriends.put(toUser, null);
					} else if (mutualFriends.get(toUser) != null) {
						mutualFriends.get(toUser).add(mutualFriend);
					}
				} else {
					if (!isFriend) {
						mutualFriends.put(toUser, new ArrayList<Long>() {
							{
								add(mutualFriend);
							}
						});
					} else {
						mutualFriends.put(toUser, null);
					}
				}
			}

			// Sorting all the Mutual friends using Tree Map
			java.util.SortedMap<Long, List<Long>> sortFriends = new TreeMap<Long, List<Long>>(new Comparator<Long>() {
				public int compare(Long key1, Long key2) {
					Integer value1 = mutualFriends.get(key1).size();
					Integer value2 = mutualFriends.get(key2).size();
					if (value1 > value2) {
						return -1;
					} else if (value1.equals(value2) && key1 < key2) {
						return -1;
					} else {
						return 1;
					}
				}
			});

			for (java.util.Map.Entry<Long, List<Long>> entry : mutualFriends.entrySet()) {
				if (entry.getValue() != null) {
					sortFriends.put(entry.getKey(), entry.getValue());
				}
			}

			Integer i = 0;
			String output = "";
			for (java.util.Map.Entry<Long, List<Long>> entry : sortFriends.entrySet()) {
				if (i == 0) {
					output = entry.getKey().toString();
				} else if (i < 10){
					output += "," + entry.getKey().toString();
				}
				++i;
			}
			context.write(key, new Text(output));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "HW1_1");
		job.setJarByClass(HW1_1.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(friendCounts.class);
		job.setMapperClass(HW1_1Mapper.class);
		job.setReducerClass(HW1_1Reducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileSystem outFs = new Path(args[1]).getFileSystem(conf);
		outFs.delete(new Path(args[1]), true);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}

