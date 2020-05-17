import java.io.*;
import java.util.Scanner;
import java.util.Vector;
import java.lang.Math.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

class Vertex implements Writable {
    public int tag; // 0 for a graph vertex, 1 for a group number
    public long group; // the group where this vertex belongs to
    public long VID = 0; // the vertex ID
    public Vector<Long> adjacent = new Vector<Long>(); // the vertex neighbors
    /* ... */

    Vertex() {
    }

    Vertex(int t, long g, long v, Vector<Long> a) {
        this.tag = t;
        this.group = g;
        this.VID = v;
        this.adjacent = a;
    }

    Vertex(int t, long grp) {
        super();
        this.tag = t;
        this.group = grp;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(tag);
        out.writeLong(group);
        out.writeLong(VID);
        int adjsize = 0;
        adjsize = this.adjacent.size();
        out.writeInt(adjsize);
        for (int i = 0; i < adjsize; i++) {
            out.writeLong(adjacent.get(i));
        }
    }

    public void readFields(DataInput in) throws IOException {
        Vector<Long> vert = new Vector<Long>();
        tag = in.readInt();
        group = in.readLong();
        VID = in.readLong();
        int adjsize = 0;
        adjsize = in.readInt();
        for (long i = 0; i < adjsize; i++) {
            vert.add(in.readLong());

        }

        this.adjacent = vert;

    }

}

public class Graph {

    public static class GraphMapper extends Mapper<Object, Text, LongWritable, Vertex> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            Vector<Long> Vert = new Vector<Long>();
            long VID = s.nextLong();
            int tagg = 0;
            while (s.hasNext()) {
                Vert.add(s.nextLong());
            }
            Vertex verprot = new Vertex(tagg, VID, VID, Vert);
            context.write(new LongWritable(VID), verprot);
            s.close();
            /* write your mapper code */
        }
    }
    /* ... */

    public static class GraphMapper2 extends Mapper<LongWritable, Vertex, LongWritable, Vertex> {
        @Override
        public void map(LongWritable key, Vertex values, Context context) throws IOException, InterruptedException {
            Vertex verprot = new Vertex(1, values.group);
            context.write(new LongWritable(values.VID), values);
            for (int i = 0; i < values.adjacent.size(); i++) {
                context.write(new LongWritable(values.adjacent.get(i)), verprot);
            }
            /* write your mapper code */
        }
    }

    public static class GraphReducer extends Reducer<LongWritable, Vertex, LongWritable, Vertex> {
        @Override
        public void reduce(LongWritable key, Iterable<Vertex> values, Context context)
                throws IOException, InterruptedException {

            Vector<Long> cloneVec = new Vector<Long>();
            long m = Long.MAX_VALUE;
            for (Vertex v : values) {
                if (v.tag == 0) {
                    cloneVec = (Vector) v.adjacent.clone(); // If we find vertex with VID
                }
                m = Math.min(m, v.group);
            }
            cloneVec.toString();
            context.write(new LongWritable(m), new Vertex(0, m, key.get(), cloneVec));
            /* write your reducer code */
        }
    }

    public static class GraphMapperFinal extends Mapper<LongWritable, Vertex, LongWritable, LongWritable> {
        @Override
        public void map(LongWritable key, Vertex value, Context context) throws IOException, InterruptedException {
            context.write(key, new LongWritable(1));
            /* write your mapper code */
        }
    }

    public static class GraphReducerFinal extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
        @Override
        public void reduce(LongWritable group, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {

            long m = 0;
            for (LongWritable v : values) {
                m = m + v.get();
            }
            context.write(group, new LongWritable(m));
            /* write your reducer code */
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("MyJob");
        /* ... First Map-Reduce job to read the graph */
        job.setJarByClass(Graph.class);
        job.setMapperClass(GraphMapper.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Vertex.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Vertex.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // job.setNumReduceTasks(0);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "/f0"));
        job.waitForCompletion(true);
        for (short i = 0; i < 5; i++) {
            Job job1 = Job.getInstance();
            /* ... Second Map-Reduce job to propagate the group number */
            job1.setJarByClass(Graph.class);
            job1.setJobName("MyJob1");
            job1.setOutputKeyClass(LongWritable.class);
            job1.setOutputValueClass(Vertex.class);
            job1.setMapOutputKeyClass(LongWritable.class);
            job1.setMapOutputValueClass(Vertex.class);
            job1.setInputFormatClass(SequenceFileInputFormat.class);
            job1.setOutputFormatClass(SequenceFileOutputFormat.class);
            job1.setMapperClass(GraphMapper2.class);
            job1.setReducerClass(GraphReducer.class);
            FileInputFormat.setInputPaths(job1, new Path(args[1] + "/f" + i));
            FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/f" + (i + 1)));
            job1.waitForCompletion(true);
        }

        Job job2 = Job.getInstance();
        /* ... Final Map-Reduce job to calculate the connected component sizes */
        job2.setJobName("MyJob2");
        job2.setJarByClass(Graph.class);
        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(LongWritable.class);
        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(LongWritable.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        job2.setMapperClass(GraphMapperFinal.class);
        job2.setReducerClass(GraphReducerFinal.class);
        FileInputFormat.setInputPaths(job2, new Path(args[1] + "/f5"));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job2.waitForCompletion(true);
    }
}
