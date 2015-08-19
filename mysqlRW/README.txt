MapReduce读写mysql数据库：
	读数据：
			下面我们来看看 DBInputFormat类的内部结构，DBInputFormat 类中包含以下三个内置类。
        1、protected class DBRecordReader implements	RecordReader< LongWritable, T>：用来从一张数据库表中读取一条条元组记录。
        2、public static class NullDBWritable implements DBWritable,Writable：主要用来实现 DBWritable 接口。DBWritable接口要实现二个函数，第一是write，第二是readFileds，这二个函数都不难理解，一个是写，一个是读出所有字段。原型如下：
				public void write(PreparedStatement statement) throwsSQLException;
				public void readFields(ResultSet result);
        3、protected static class DBInputSplit implements InputSplit：主要用来描述输入元组集合的范围,包括 start 和 end 两个属性，start 用来表示第一条记录的索引号，end 表示最后一条记录的索引号。
        下面对怎样使用 DBInputFormat 读取数据库记录进行详细的介绍，具体步骤如下：
          步骤一	、配置 JDBC 驱动、数据源和数据库访问的用户名和密码。代码如下。
				DBConfiguration.configureDB (Job job, StringdriverClass, String dbUrl, String userName, String passwd)
        		MySQL 数据库的 JDBC 的驱动为“com.mysql.jdbc.Driver”，数据源为“jdbc:mysql://localhost/testDB”，其中testDB为访问的数据库。useName一般为“root”，passwd是你数据库的密码。
        步骤二、使用 setInput 方法操作 MySQL 中的表，setInput 方法的参数如下。
				 DBInputFormat.setInput(Job job, Class< extends DBWritable> inputClass, String tableName, String conditions,String orderBy, String... fieldNames)
        		这个方法的参数很容易看懂，inputClass实现DBWritable接口。string tableName表名， conditions表示查询的条件，orderby表示排序的条件，fieldNames是字段，这相当与把sql语句拆分的结果。当然也可以用sql语句进行重载，代码如下：
					setInput(Job job, Class< extends DBWritable> inputClass, String inputQuery, StringinputCountQuery)。
        步骤三、编写MapReduce函数，包括Mapper 类、Reducer 类、输入输出文件格式等，然后调用job.waitForCompletion(true)
        
        写数据：
        数据处理结果的数据量一般不会太大，可能适合hadoop直接写入数据库中。hadoop提供了数据库接口，把 MapReduce 的结果直接输出到 MySQL、Oracle 等数据库。 主要的类如下所示。
        1、DBOutFormat: 提供数据库写入接口。
        2、DBRecordWriter:提供向数据库中写入的数据记录的接口。
        3、DBConfiguration:提供数据库配置和创建链接的接口。