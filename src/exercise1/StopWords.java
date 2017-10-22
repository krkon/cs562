package exercise1;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

public class StopWords extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(StopWords.class);
  
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new StopWords(), args);
    System.exit(res);
  }


  private long time1;
  private long time2;
  
  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf(), "wordcount");

   
    for (int i = 0; i < args.length; i += 1) {
      if ("-skip".equals(args[i])) {
        job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
        i += 1;
        job.addCacheFile(new Path(args[i]).toUri());
        // this demonstrates logging
        LOG.info("Added file to the distributed cache: " + args[i]);
      }
    }
    job.setJarByClass(this.getClass());
    
    // Use TextInputFormat, the default unless job.setInputFormatClass is used
    int k;
    for(k = 0; k < args.length - 1; k++){
    	FileInputFormat.addInputPath(job, new Path(args[k]));
    }
    FileOutputFormat.setOutputPath(job, new Path(args[k]));

    
    job.setMapperClass(Map.class);
    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    
    time1 = System.nanoTime();
    job.waitForCompletion(true);
    time2 = System.nanoTime();
    
    
    Job secondJob = Job.getInstance(getConf(), "job2");
    FileInputFormat.addInputPath(secondJob, new Path(args[k]+"/part-r-00000"));
    FileOutputFormat.setOutputPath(secondJob, new Path("stopwordsResults"));
    secondJob.setJarByClass(this.getClass());
    
    secondJob.setMapperClass(MapResult.class);
    secondJob.setCombinerClass(ReduceResult.class);
    secondJob.setReducerClass(ReduceResult.class);
    secondJob.setOutputKeyClass(Text.class);
    secondJob.setOutputValueClass(IntWritable.class);
    
    LOG.info("Time taken: " + (time2 - time1)/1000000000.0 +"sec");
    
    return secondJob.waitForCompletion(true) ? 1 : 0;
    
  }

  private static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private boolean caseSensitive = false;
    private String input;
    private Set<String> patternsToSkip = new HashSet<String>();
    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
    private final String regex = "[a-zA-z]+";

    protected void setup(Mapper.Context context)
        throws IOException,
        InterruptedException {
      if (context.getInputSplit() instanceof FileSplit) {
        this.input = ((FileSplit) context.getInputSplit()).getPath().toString();
      } else {
        this.input = context.getInputSplit().toString();
      }
      Configuration config = context.getConfiguration();
      this.caseSensitive = config.getBoolean("wordcount.case.sensitive", false);
      if (config.getBoolean("wordcount.skip.patterns", false)) {
        URI[] localPaths = context.getCacheFiles();
        parseSkipFile(localPaths[0]);
      }
     
    }

    private void parseSkipFile(URI patternsURI) {
      LOG.info("Added file to the distributed cache: " + patternsURI);
      try {
        BufferedReader fis = new BufferedReader(new FileReader(new File(patternsURI.getPath()).getName()));
        String pattern;
        while ((pattern = fis.readLine()) != null) {
          patternsToSkip.add(pattern);
        }
      } catch (IOException ioe) {
        System.err.println("Caught exception while parsing the cached file '"
            + patternsURI + "' : " + StringUtils.stringifyException(ioe));
      }
    }

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      String line = lineText.toString();
      if (!caseSensitive) {
        line = line.toLowerCase();
      }
      Text currentWord = new Text();
      for (String word : WORD_BOUNDARY.split(line)) {
        if (word.isEmpty() || !word.matches(regex) || patternsToSkip.contains(word)) {
            continue;
        }
            currentWord = new Text(word);
            context.write(currentWord,one);
        }             
    }
  }

  private static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text word, Iterable<IntWritable> counts, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable count : counts) {
        sum += count.get();
      }
      context.write(word, new IntWritable(sum));
    }
  }

  private static class MapResult extends Mapper<LongWritable, Text, Text, IntWritable> {
	  
	  private static final Pattern WORD_BOUNDARY = Pattern.compile("\\b");
	  
	  
	  public void map(LongWritable offset, Text lineText, Context context)
		        throws IOException, InterruptedException {
		  		String line = lineText.toString();
		  		
		  		String[] str = new String[2];
		  		str = line.split("\t");
		  		
		  		Text term = new Text();
		  		term = new Text(str[0]);
		  		
		  		IntWritable frequency = new IntWritable(0);
		  		frequency.set(Integer.parseInt(str[1]));
		  		
		  		
		  		if(frequency.get() > 4000){
		  			context.write(term, frequency);
		  		}
	  }
  }
  
  private static class ReduceResult extends Reducer<Text, IntWritable, Text, IntWritable> {
	  private HashMap<Text, Integer> list = new HashMap();



		private static HashMap<Text, Integer> sortList(HashMap<Text, Integer> map) {
			
			LinkedList<Entry<Text, Integer>> linkedlist = new LinkedList<Entry<Text, Integer>>(map.entrySet());
			
			Collections.sort(linkedlist, new Comparator<Entry<Text, Integer>>()
			{
			            public int compare(Entry<Text, Integer> o1, Entry<Text, Integer> o2)
			            {
			            	return o2.getValue().compareTo(o1.getValue());
			            }
			 });
			
			HashMap<Text, Integer> sortedMap = new LinkedHashMap<Text, Integer>();
			
			for(Entry<Text, Integer> entry : linkedlist)
			{
				sortedMap.put(entry.getKey(), entry.getValue());
			}
			
			return sortedMap;
		}



		@Override
	    public void reduce(Text word, Iterable<IntWritable> counts, Context context)
	        throws IOException, InterruptedException {
	      int sum = 0;
	      for (IntWritable count : counts) {
	        sum += count.get();
	      }
	      list.put(new Text(word), new Integer(sum));
	    }
	  
  
  	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
  		
  		FileSystem fs = FileSystem.get(context.getConfiguration());
  		
  		Path path = new Path("stopwords.csv");
  		if(fs.exists(path)){
  			fs.delete(path, true);
  		}
  		FSDataOutputStream outStream = fs.create(path);
  		
  		
		HashMap<Text, Integer> sortedMap = sortList(list);
		int k = 0;
		for(Entry<Text, Integer> t : sortedMap.entrySet()){
			context.write(t.getKey(), new IntWritable(t.getValue()));
			outStream.writeUTF(t.getKey().toString()+"\n");
			if(k++ < 10)
				LOG.info(t.getKey() + " " +t.getValue().toString());
		}
		
  		outStream.close();
  		fs.close();
	}
  }
}
