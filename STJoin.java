import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class STJoin {
    // Mapper：把每一行拆成两条记录，标记1代表左表（孩子），标记2代表右表（父母）
    public static class STJoinMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().trim().split("\\s+");
            // 过滤表头和空行
            if (split.length < 2 || split[0].equals("child")) return; 
            String child = split[0];
            String parent = split[1];
            // 左表：以parent为key，标记1表示这个key是中间人，其孩子是child
            context.write(new Text(parent), new Text("1 " + child)); 
            // 右表：以child为key，标记2表示这个key是中间人，其父母是parent
            context.write(new Text(child), new Text("2 " + parent)); 
        }
    }
    
    // Reducer：收到同一个中间人的所有关系，做笛卡尔积
    public static class STJoinReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> grandchild = new ArrayList<>();
            List<String> grandparent = new ArrayList<>();
            for (Text val : values) {
                String[] parts = val.toString().split(" ");
                if ("1".equals(parts[0])) {
                    grandchild.add(parts[1]);   // 左表：该key的孩子（即孙子辈）
                } else {
                    grandparent.add(parts[1]);  // 右表：该key的父母（即祖父母辈）
                }
            }
            // 笛卡尔积：每个孙子 匹配 每个祖父母
            for (String gc : grandchild) {
                for (String gp : grandparent) {
                    context.write(new Text(gc), new Text(gp));
                }
            }
        }
    }
    
    // Driver：作业入口
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "STJoin");
        job.setJarByClass(STJoin.class);
        job.setMapperClass(STJoinMapper.class);
        job.setReducerClass(STJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
