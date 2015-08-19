package cn.hadoop.liuyu.mysqlRW;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class MysqlWrite {
	public static class MysqlMapper extends Mapper< LongWritable,Text,Text,Text>
	   {  
	        public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException
	        {  
	            //读取 hdfs 中的数据
	            String email = value.toString().split("\\s")[0];
	            String name = value.toString().split("\\s")[1];
	            context.write(new Text(email),new Text(name));  
	        }  
	}  
	public static class MysqlReducer extends Reducer< Text,Text,UserRecord,UserRecord>
	    {  
	        public void reduce(Text key,Iterable< Text> values,Context context)throws IOException,InterruptedException
	        {  
	        //接收到的key value对即为要输入数据库的字段，所以在reduce中：
	        //wirte的第一个参数，类型是自定义类型UserRecord，利用key和value将其组合成UserRecord，然后等待写入数据库
	        //wirte的第二个参数，wirte的第一个参数已经涵盖了要输出的类型，所以第二个类型没有用，设为null
	        for(Iterator< Text> itr = values.iterator();itr.hasNext();)
	                 {  
	        			String s=itr.next().toString();
	                     context.write(new UserRecord(key.toString(),s),null);
	                 }  
	        }  
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		 	Configuration conf = new Configuration();
	        //配置 JDBC 驱动、数据源和数据库访问的用户名和密码
	        DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver","jdbc:mysql://localhost:3306/liuyu_www","root", "12035318");    
	        Job job = Job.getInstance(conf);//新建一个任务 
	        job.setJarByClass(MysqlWrite.class);//主类  
	          
	        job.setMapperClass(MysqlMapper.class); //Mapper 
	        job.setReducerClass(MysqlReducer.class); //Reducer 
	          
	        job.setOutputKeyClass(Text.class);  
	        job.setOutputValueClass(Text.class);
	        
	        
	        job.setOutputFormatClass(DBOutputFormat.class);//向数据库写数据
	        //输入路径
	        FileInputFormat.addInputPath(job, new Path("hdfs://master:9000/mysql/data/data.txt"));
	        //设置输出到数据库    表名：test  字段：uid、email、name
	        DBOutputFormat.setOutput(job, "test", "uid","email","name");
	        System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
