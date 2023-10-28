package ma.enset.logs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LogsAnalysis {

    public class LogsMapper extends Mapper<LongWritable, Text,Text,BooleanWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] components = value.toString().split(" ");
            //["192.168.1.1","-","-","[12/May/2023:15:30:45","+0000]",""GET","/page1","HTTP/1.1"","200","1234"]
            String AddressIP = components[0];
            String StatusRequest=components[8];

            Boolean StatusSucces=false;
            if(StatusRequest.equals("200")){
                StatusSucces=true;
            }

            context.write(new Text(AddressIP), new BooleanWritable(StatusSucces));
        }

    }

    public class LogsReducer extends Reducer<Text, BooleanWritable,Text,Text> {
        public void reduce(Text key, Iterable<BooleanWritable> values, Context context) throws IOException, InterruptedException {
            int totalRequest = 0;
            int totalHttpSucess = 0;
            for (BooleanWritable value : values) {
                String s = value.toString();
                if(s=="true"){
                    totalHttpSucess+=1;
                }
                totalRequest += 1;

            }
            context.write(key , new Text(totalRequest+"      "+totalHttpSucess));
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);
        job.setJar("mape-reduce-1.0-SNAPSHOT.jar");

        //Les classes Mapper et Reducer
        job.setMapperClass(LogsMapper.class);
        job.setReducerClass(LogsReducer.class);

        //Les types de sortie du Mapper et du reducer
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BooleanWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //Le format d'entrée
        job.setInputFormatClass(TextInputFormat.class);

        //le path des Fichiers d'entrées
        TextInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.waitForCompletion(true);
    }
}
