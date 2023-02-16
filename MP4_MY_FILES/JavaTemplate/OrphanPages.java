import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.*;

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

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(IntArrayWritable.class);

        job.setMapperClass(LinkCountMap.class);
        job.setReducerClass(OrphanPageReduce.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(OrphanPages.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(Integer[] numbers) {
            super(IntWritable.class);
            IntWritable[] ints = new IntWritable[numbers.length];
            for (int i = 0; i < numbers.length; i++) {
                ints[i] = new IntWritable(numbers[i]);
            }
            set(ints);
        }
    }
    public static class LinkCountMap extends Mapper<Object, Text, NullWritable, IntArrayWritable> {
        private TreeSet <Pair<Integer, Integer>> countToTitleMap = new TreeSet<Pair<Integer, Integer>>();
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().trim().split(":");
            String pageId = line[0];
            String[] links = line[1].trim().split(" ");
            for(int i = 1; i < links.length; i++){
                if(!pageId.equals(links[i])){
                    countToTitleMap.add(new Pair<Integer, Integer>(Integer.parseInt(pageId), Integer.parseInt(links[i])));
//                    context.write(new IntWritable(Integer.parseInt(pageId)), new IntWritable(Integer.parseInt(links[i])));
                }
                else{
                    countToTitleMap.add(new Pair<Integer, Integer>(Integer.parseInt(pageId),-1));
//                    context.write(new IntWritable(Integer.parseInt(pageId)), new IntWritable(-1));
                }
            }
        }


        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Pair<Integer, Integer> item : countToTitleMap) {
                Integer[] links = {item.second, item.first};
                IntArrayWritable val = new IntArrayWritable(links) ;
                context.write(NullWritable.get(), val);
            }
        }
    }

    public static class OrphanPageReduce extends Reducer<IntWritable, IntWritable, IntWritable, NullWritable> {
        private TreeSet <IntWritable> leftSide = new TreeSet<IntWritable>();
        private TreeSet <IntWritable> rightSide = new TreeSet<IntWritable>();
        private TreeSet<IntWritable> difference = new TreeSet<IntWritable>();
        @Override
        public void reduce(NullWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Iterator<IntArrayWritable> mainIterator = values.iterator();
            while(mainIterator.hasNext()){
                IntWritable[] pair =(IntWritable[]) mainIterator.next().toArray();
                IntWritable pageId = new IntWritable(Integer.parseInt(pair[0].toString()));
                IntWritable linksTo = new IntWritable(Integer.parseInt(pair[1].toString()));
                leftSide.add(pageId);
                rightSide.add(linksTo);
            }

            for(IntWritable element : leftSide){
                if(!rightSide.contains(element)){
                    difference.add(element);
                }
            }
            for(IntWritable element: difference){
                context.write(element, NullWritable.get());
            }
        }
    }
}

class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}
