package exercise2;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import exercise1.StopWords;

public class InvertIndex extends Configured implements Tool {
	
	private static final Logger LOG = Logger.getLogger(StopWords.class);

	@Override
	public int run(String[] args) throws Exception {
		
		Job job = Job.getInstance(getConf(), "invertIndexJob");
		Job secondjob = Job.getInstance(getConf(), "invertIndexJobSecond");
		int i = 0;
		
		job.setJarByClass(this.getClass());
	    secondjob.setJarByClass(this.getClass());
		
	    for(i = 0; i < args.length - 1; i++){
			FileInputFormat.addInputPath(job, new Path(args[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(args[i]));
		FileInputFormat.addInputPath(secondjob, new Path("ex2intermidiate/part-r-00000"));
		FileOutputFormat.setOutputPath(secondjob, new Path("ex2final"));
		
		
		job.setMapperClass(Map.class);
	    job.setCombinerClass(Reduce.class);
	    job.setReducerClass(Reduce.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
		
	    job.waitForCompletion(true);
	    
		secondjob.setMapperClass(IdMap.class);
		secondjob.setCombinerClass(IdReduce.class);
		secondjob.setReducerClass(IdReduce.class);
		secondjob.setOutputKeyClass(Text.class);
		secondjob.setOutputValueClass(Text.class);
	    
		return secondjob.waitForCompletion(true) ? 1 : 0;
	    
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new InvertIndex(), args);
		System.exit(res);
	}
	
	private static class Map extends Mapper<LongWritable, Text, Text, Text>{
		
	    private boolean caseSensitive = false;
	    private String input;
	    private Set<String> patternsToSkip = new HashSet<String>();
	    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
	    private final String regex = "[a-zA-z]+";
		private ArrayList<String> stopwords = new ArrayList<String>();
		
		
		private void initializeStowords(ArrayList<String> stopwords, Context context) {
	    	Path stopwordspath = new Path("stopwords.csv");
	    	String input = new String();
	    	try{
	    		FileSystem fs = FileSystem.get(context.getConfiguration());
	    		if(fs.exists(stopwordspath)){
	    			FSDataInputStream in = fs.open(stopwordspath);
	    			BufferedReader buf = new BufferedReader(new InputStreamReader(in));
	    			while((input = buf.readLine()) != null){
	    				stopwords.add(input.substring(2));
	    			}
		    		in.close();
		    		
	    		}
	    		
	    		fs.close();
	    	}
	    	catch(Exception exception){
	    		System.err.println("Exception: " + StringUtils.stringifyException(exception));
	    	}
		}
		
		
		
		protected void setup(Mapper.Context context)
			        throws IOException,
			        InterruptedException {
		if (context.getInputSplit() instanceof FileSplit) {
	        this.input = ((FileSplit) context.getInputSplit()).getPath().toString();
	      } else {
	        this.input = context.getInputSplit().toString();
	      }
	      Configuration config = context.getConfiguration();
	      this.caseSensitive = config.getBoolean("invertIndex.case.sensitive", false);
	      if (config.getBoolean("invertIndex.skip.patterns", false)) {
	        URI[] localPaths = context.getCacheFiles();
	        parseSkipFile(localPaths[0]);
	      }
	      initializeStowords(stopwords, context);
	      config.setInt("words", 0);
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
	      
	      
	      int numofwords;
	      if (!caseSensitive) {
	        line = line.toLowerCase();
	      }
	      Text currentWord = new Text();
	     
	      String file = ((FileSplit) context.getInputSplit()).getPath().getName();
	      numofwords = context.getConfiguration().getInt("integer", 0);
	      for (String word : WORD_BOUNDARY.split(line)) {
	    	  numofwords++;
	    	  
	        if (word.isEmpty() || !word.matches(regex) || stopwords.contains(word)) {
	            continue;
	        }
	            currentWord = new Text(word);
	            context.write(currentWord,new Text(file));
	            
	        }
	      context.getConfiguration().setInt("integer", numofwords);
	    }
	    
	    @Override
		protected void cleanup(
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			FileSystem fs = FileSystem.get(context.getConfiguration());
			Path path = new Path(((FileSplit) context.getInputSplit()).getPath().getName() + "Unique");
	  		
			if(fs.exists(path)){
	  			fs.delete(path, true);
	  		}
	  		
			FSDataOutputStream outStream = fs.create(path);
	  		outStream.writeInt(context.getConfiguration().getInt("integer", -1));
	  		
	  		outStream.close();
	  		fs.close();
	  		
			super.cleanup(context);
		}
	    
	    
	}
	
	
	private static class Reduce extends Reducer<Text, Text, Text, Text>{
		
		
		 @Override
		    public void reduce(Text word, Iterable<Text> docs, Context context)
		        throws IOException, InterruptedException {
		      
		      StringBuilder str = new StringBuilder();
		      String previous = new String();
		      
		      for (Text doc : docs) {
		    	  
		    	  if(previous.equals(doc.toString()) || doc.getLength() == 0){
		    		  continue;
		    	  }
		    	  previous = new String(doc.toString());
		    	  str.append(" "+previous);
		      }
		      context.write(word, new Text(str.toString()));
		    }
		 
	}
	
	
	private static class IdMap extends Mapper<LongWritable, Text, Text, Text>{
		
	    private boolean caseSensitive = false;
	    private String input;
		
		protected void setup(Mapper.Context context)
		        throws IOException,
		        InterruptedException {
			if (context.getInputSplit() instanceof FileSplit) {
				this.input = ((FileSplit) context.getInputSplit()).getPath().toString();
			} else {
				this.input = context.getInputSplit().toString();
			}
			Configuration config = context.getConfiguration();
			
			config.setInt("counter", 0);
		}
		
		
		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
		      String line = lineText.toString();
		      int counter = context.getConfiguration().getInt("counter", 0);
		      if (!caseSensitive) {
		        line = line.toLowerCase();
		      }

		      String[] splitLine = new String[2];
		      splitLine = line.split("\t");
		      
		      Text mKey;		      
		      mKey = new Text(splitLine[0]);
		     
		      Text values;
		      values = new Text(splitLine[1]);
		      
		      counter++;
		      
		      context.getConfiguration().setInt("counter", counter);	         
		      context.write(mKey,new Text(values + " " + counter));             
		    }
		
		
	}
	
	
	private static class IdReduce extends Reducer<Text, Text, Text, Text>{
		
		
		 @Override
		    public void reduce(Text word, Iterable<Text> docs, Context context)
		        throws IOException, InterruptedException {
		      StringBuilder str = new StringBuilder();

		      for (Text doc : docs) {
		    	  str.append(" "+ doc);
		      }
		      context.write(word, new Text(str.toString()));
		    }
	}
}
