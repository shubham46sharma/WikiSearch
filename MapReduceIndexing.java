package HadoopInvertedIndex;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapReduceIndexing {
    public static class IndexMapper extends Mapper<Object, Text, Text, Text> {
        private Text word = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//            System.out.println("Start--------------");
            String filePath=context.getInputSplit().toString().split(":")[2];
            String[] splitFilePath=filePath.split("/");
            String docId = splitFilePath[splitFilePath.length-1];



            StringTokenizer itr = new StringTokenizer(value.toString(), " '-");
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase());
                if(!word.toString().equals("") && !(word.toString().trim().equals(""))){
                    context.write(word, new Text(docId));
                }
            }
//            System.out.println("End ----------");
        }
    }

    public static class IndexReducer extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<String,Integer> wordToken = new HashMap<>();
            for (Text val : values) {
                if (wordToken.containsKey(val.toString())) {
                    wordToken.put(val.toString(), wordToken.get(val.toString()) + 1);
                } else {
                    wordToken.put(val.toString(), 1);
                }
            }
            StringBuilder frequencyIndex = new StringBuilder();
            for(String docID : wordToken.keySet()){
                frequencyIndex.append(docID).append(":").append(wordToken.get(docID)).append(" ");
            }
            context.write(key, new Text(frequencyIndex.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("MapReduceIndexing");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Mapreduce Indexing");
//        job.setNumReduceTasks(60);
        job.setJarByClass(MapReduceIndexing.class);
        job.setMapperClass(IndexMapper.class);
        job.setReducerClass(IndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("./src/Resources/Input/CrawlData2/"));
        FileOutputFormat.setOutputPath(job, new Path("./src/Resources/Output/MapReduceIndexing"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
