import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Sort {
    // Mapper：读取每一行数字，作为Key发出去，Value设为1
    public static class SortMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        private IntWritable data = new IntWritable();
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            data.set(Integer.parseInt(value.toString().trim()));
            context.write(data, new IntWritable(1));
        }
    }
    // Reducer：Hadoop默认会把Key按从小到大排序，我们只需要按顺序编号
    public static class SortReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private static int lineNum = 1; // 全局计数器，记录位次
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            context.write(new IntWritable(lineNum), key); // 输出：位次 原数字
            lineNum++;
        }
    }
    // Driver：作业的入口，负责指挥
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Sort");
        job.setJarByClass(Sort.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        // 强制只用一个Reducer，保证全局有序且位次连续
        job.setNumReduceTasks(1); 
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
