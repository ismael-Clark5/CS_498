import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

public class OrphanPages extends Configured implements Tool {
    public static final Log LOG = LogFactory.getLog(OrphanPages.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new OrphanPages(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(this.getConf(), "Orphan Pages");
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setMapperClass(LinkCountMap.class);
        job.setReducerClass(OrphanPageReduce.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(OrphanPages.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(":");
            String pageId = line[0];
            String[] links = line[1].split(" ");
            for(int i = 1; i < links.length; i++){
                if(!pageId.equals(links[i])){
                    context.write(new IntWritable(Integer.parseInt(pageId)), new IntWritable(Integer.parseInt(links[i])));
                }
                else{
                    context.write(new IntWritable(Integer.parseInt(pageId)), new IntWritable(-1));
                }
            }
        }
    }

    public static class OrphanPageReduce extends Reducer<IntWritable, IntWritable, IntWritable, NullWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            TreeSet<String> leftSide = new TreeSet<String>();
            TreeSer<String> rightSide = new TreeSet<String>();
            for(TextArrayWritable val : values){
                Text[] valuesPair = (Text[]) values.toArray();
                leftSide.add(Integer.parseInt(valuesPair[0].toString()));
                rightSide.add(Integer.parseInt(valuesPair[1].toString()));
            }
            TreeSet<String> difference = new TreeSet<String>();
            for(String element : leftSide){
                if(!rightSide.contains(element)){
                    difference.add(element);
                }
            }
            for(String element: difference){
                context.write(element);
            }
        }
    }
}
