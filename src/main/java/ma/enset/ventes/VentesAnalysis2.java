package ma.enset.ventes;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class VentesAnalysis2 {

    public static class VentesMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
        private Text villeProduit = new Text();
        private FloatWritable prix = new FloatWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(" ");
            String ville = fields[1];
            String produit = fields[2];
            float prixUnitaire = Float.parseFloat(fields[3]);
            int annee = Integer.parseInt(fields[0].substring(0, 4));
            if(annee == 2021) {
                villeProduit.set(ville + " " + produit);
                prix.set(prixUnitaire);
                context.write(villeProduit, prix);
            }
        }
    }

    public static class VentesReducer extends Reducer<Text,FloatWritable,Text,FloatWritable> {
        private FloatWritable totalPrix = new FloatWritable();
        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float sum = 0;
            for (FloatWritable val : values) {
                sum += val.get();
            }
            totalPrix.set(sum);
            context.write(key, totalPrix);
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
        job.setMapOutputValueClass(FloatWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        //Le format d'entrée
        job.setInputFormatClass(TextInputFormat.class);

        //le path des Fichiers d'entrées
        TextInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.waitForCompletion(true);
    }
}
