package cn.hadoop.liuyu.mysqlRW;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

@SuppressWarnings("deprecation")
public class MysqlRead {

	public static class MysqlMapper extends Mapper<LongWritable,UserRecord,Text,Text>{

		@Override
		protected void map(LongWritable key, UserRecord values, Context context)
				throws IOException, InterruptedException {
			//从 mysql 数据库读取需要的数据字段
			context.write(new Text(values.uid+""), new Text(values.name +" "+values.email));
		}
	}
	
	public static class MysqlReducer extends Reducer<Text,Text,Text,Text>{

		@Override
		protected void reduce(Text key, Iterable<Text>  values,Context context)
				throws IOException, InterruptedException {
			//将数据输出到HDFS中
			for(;values.iterator().hasNext();){
				context.write(key,values.iterator().next());
			}
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
        //输出路径
        Path output = new Path("hdfs://master:9000/mysql/out");
        
        FileSystem fs = FileSystem.get(URI.create(output.toString()), conf);
        if (fs.exists(output)) {
                fs.delete(output,true);
        }
        
        //mysql的jdbc驱动
        DistributedCache.addFileToClassPath(new Path("hdfs://master:9000/jar/mysql-connector-java-5.1.14.jar"), conf);  //指定mysql的jdbc的jar位置
        //设置mysql配置信息   4个参数分别为： Configuration对象、mysql数据库地址、用户名、密码
        DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/liuyu_www", "root", "12035318");  
        
        Job job = Job.getInstance(conf);//新建一个任务
        job.setJarByClass(MysqlRead.class);//主类
        
        job.setMapperClass(MysqlMapper.class);//Mapper
        job.setReducerClass(MysqlReducer.class);//Reducer
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setInputFormatClass(DBInputFormat.class);//从数据库中读取数据
        FileOutputFormat.setOutputPath(job, output);
        
        //列名
        String[] fields = { "uid", "email","name" }; 
        //六个参数分别为：
        //1.Job;2.Class< extends DBWritable> 3.表名;4.where条件 5.order by语句;6.列名
       DBInputFormat.setInput(job, UserRecord.class,"user", null, null, fields);           
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
