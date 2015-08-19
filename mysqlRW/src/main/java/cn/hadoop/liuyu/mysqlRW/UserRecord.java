package cn.hadoop.liuyu.mysqlRW;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

/**
 * 实现DBwrite和Writable接口
 * @author liuyu
 *
 */
public class UserRecord  implements  DBWritable ,Writable{

	int uid;
	String email;
	String name;
	
	public UserRecord(String email,String name){
		//this.uid=uid;
		this.email=email;
		this.name=name;
	}
	
	//从数据库读取所需要的字段
	public void readFields(ResultSet resultSet) throws SQLException {
		// TODO Auto-generated method stub
		this.uid=resultSet.getInt(1);
		this.email=resultSet.getString(2);
		this.name=resultSet.getString(3);
	}

	//向数据库写入数据
	public void write(PreparedStatement statement) throws SQLException {
		// TODO Auto-generated method stub
		statement.setInt(1, this.uid);
		statement.setString(2,this.email);
		statement.setString(3,this.name);
	}

	//读取序列化数据，即反序列化
	public void readFields(DataInput in) throws IOException {
		// TODO Auto- method stub
		this.uid=in.readInt();
		this.email=in.readUTF();
		this.name=in.readUTF();
	}
	
	//序列化数据
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(uid);
		out.writeUTF(email);
		out.writeUTF(name);
	}

	public String toString(){
		return new String(this.uid+" "+this.email+" "+this.name);
	}
}
