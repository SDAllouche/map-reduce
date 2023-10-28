package ma.enset.wordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;

public class WordCount {

    public static class ClassMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");
            for (String word:words){

                context.write(new Text(word),new IntWritable(1));
            }
        }
    }

    public static class ClassReducer extends Reducer<Text, IntWritable,Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable,Text, IntWritable>.Context context) throws IOException, InterruptedException {
            Iterator<IntWritable> it= values.iterator();
            int sum = 0;
            while (it.hasNext()) {
                sum += it.next().get();

            }

            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);
        job.setJar("mape-reduce-1.0-SNAPSHOT.jar");

        //Les classes Mapper et Reducer
        job.setMapperClass(ClassMapper.class);
        job.setReducerClass(ClassReducer.class);

        //Les types de sortie du Mapper et du reducer
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //Le format d'entrée
        job.setInputFormatClass(TextInputFormat.class);

        //le path des Fichiers d'entrées
        TextInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.waitForCompletion(true);
    }
}
