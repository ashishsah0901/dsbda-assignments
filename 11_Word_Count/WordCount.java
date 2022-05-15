public class WordCount {

  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);

      while(tokenizer.hasMoreTokens()) {
        word.set(tokenizer.nextToken());
        context.write(word, one);
      }
    }
  }

  public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

      int sum = 0;

      for(IntWritable val: values) {
        sum += val.get();
      }

      context.write(key, new IntWritable(sum));
    }
  }

  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    Job job = new Job(conf, "wordcount");

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(Map.class);
    job.setReducerClass(Reducer.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);
  }
}

/**
 * Compile WordCount.java and create a jar:
 * bin/hadoop com.sun.tools.javac.Main WordCount.java
 * jar cf wc.jar WordCount*.class

 * Sample text file as input:
 * bin/hadoop fs -ls /user/wordcount/input/
 * bin/hadoop fs -cat /user/wordcount/input/file01
 * bin/hadoop fs -cat /user/wordcount/input/file02

 * Output
 * bin/hadoop jar wc.jar WordCount /user/wordcount/input /user/wordcount/output
 * bin/hadoop fs -cat /user/wordcount/output/part-r-00000
 */