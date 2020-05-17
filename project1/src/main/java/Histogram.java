import java.io.*;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

/* single color intensity */
class Color implements WritableComparable<Color> {
    public int type; /* red=1, green=2, blue=3 */
    public int intensity; /* between 0 and 255 */

    private Color() {
    }

    Color(int t, int i) {
        this.type = t;
        this.intensity = i;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(type);
        out.writeInt(intensity);
    }

    public void readFields(DataInput in) throws IOException {
        type = in.readInt();
        intensity = in.readInt();
    }

    public String toString() {
        return type + " " + intensity;
    }

    public int compareTo(Color current) {
        int currentType = this.type;
        int currentIntensity = this.intensity;
        int tempType = current.type;
        int tempIntensity = current.intensity;

        if (currentType == tempType) {
            if (currentIntensity > tempIntensity) {
                return 1;
            } else if (currentIntensity == tempIntensity) {
                return 0;
            } else {
                return -1;
            }
        } else if (currentType > tempType) {
            return 1;

        } else {
            return -1;
        }

    }

    /*
     * need class constructors, toString, write, readFields, and compareTo methods
     */
}

public class Histogram {
    public static class HistogramMapper extends Mapper<Object, Text, Color, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int red = s.nextInt();
            int green = s.nextInt();
            int blue = s.nextInt();
            context.write(new Color(1, red), new IntWritable(1));
            context.write(new Color(2, green), new IntWritable(1));
            context.write(new Color(3, blue), new IntWritable(1));
            s.close();
            /* write your mapper code */
        }
    }

    public static class HistogramReducer extends Reducer<Color, IntWritable, Color, LongWritable> {
        @Override
        public void reduce(Color key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable v : values) {
                sum += 1;
            }
            ;
            context.write(key, new LongWritable(sum));
            /* write your reducer code */
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("MyJob");
        job.setJarByClass(Histogram.class);
        job.setOutputKeyClass(Color.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(Color.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(HistogramMapper.class);
        job.setReducerClass(HistogramReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true); /* write your main program code */
    }
}
