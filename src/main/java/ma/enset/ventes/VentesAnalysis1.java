package ma.enset.ventes;


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

public class VentesAnalysis1 {

    public static class VentesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable vente = new IntWritable(1);
        private Text ville = new Text();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(" ");
            ville.set(fields[1]);
            context.write(ville, vente);
        }
    }

    public static class VentesReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable totalVentes = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            totalVentes.set(sum);
            context.write(key, totalVentes);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);
        job.setJar("mape-reduce-1.0-SNAPSHOT.jar");

        //Les classes Mapper et Reducer
        job.setMapperClass(VentesMapper.class);
        job.setReducerClass(VentesReducer.class);

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
