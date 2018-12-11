import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;


@SuppressWarnings("deprecation")
public class NegativeReviewWordCount {

    public static class BusinessReviewMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private final static String[] words = new String[] {"a", "and", "an", "but", "is", "or", "the", "to", "too", ".",
            ",", "!", "were", "are", "then", "also", "with", "while", "who", "which", "besides", "since", "until",
            "after", "before", "like", "so", "they", "them", "their", "there", "they're", "we", "you", "i"};
    private final static Set<String> wordsToSkip = new HashSet<>(Arrays.asList(words));

        public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {
            String[] singleReview = value.toString().split("\\|");

            if(singleReview.length >= 9){
                // 3th index is the review
                StringTokenizer reviewItr = new StringTokenizer(singleReview[3], " ,.1234567890!;?-:@[](){}_*/");
                String word = "";

                // 8th index is the star rating
                if (Integer.parseInt(singleReview[8]) < 3) {
                    while(reviewItr.hasMoreTokens()) {
                        word = reviewItr.nextToken().toLowerCase();

                        if (!wordsToSkip.contains(word)) {
                            // 4th index is the businessID
                            context.write(new Text(singleReview[4] + ' ' + word), one);
                        }

                    }

                }
            }

        }

    }

    public static class BusinessReviewReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
            private IntWritable result = new IntWritable();

            public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                int sum = 0;
                for (IntWritable val : values) {
                    sum += val.get();
                }
                result.set(sum);
                context.write(key, result);
            }
        }



    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "yelp negative word count");
        job.setJarByClass(NegativeReviewWordCount.class);

        job.setMapperClass(BusinessReviewMapper.class);
        job.setReducerClass(BusinessReviewReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}

