package com.cn.derby;
 
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cn.domain.Student;
import com.cn.util.YuFormatUtil;
 
/**
 * 
 * @author hou
 *
 */
public class DerbyJDBC {
	private Logger logger = LoggerFactory.getLogger(DerbyJDBC.class);

	static YuFormatUtil bsUtil = new YuFormatUtil();
	/**
	 * @param args
	 */
	public void operationDB() {
		try {
			Connection conn = bsUtil.getDerbyConn();
			Statement state = conn.createStatement();
			state.executeUpdate("create table student(id varchar(128),name varchar(128),brithday varchar(128),sex int,address varchar(128))");
			state.close();
			
//			Statement state = conn.createStatement();
//			state.executeUpdate("create table derbytable(id int,val varchar(128))");
//			state.close();
			
//			Statement state2 = conn.createStatement();
//			state2.executeUpdate("insert into derbytable values (1,'tom') ");
//			state2.executeUpdate("insert into derbytable values (2,'jerry') ");
//			state2.close();
			
//			PreparedStatement pstate1 = conn.prepareStatement("select * from derbytable where id = ?");
//			pstate1.setInt(1, 2);
//			ResultSet rset1 = pstate1.executeQuery();
//			while(rset1.next()) {
//				System.out.println(rset1.getInt(1)+">"+rset1.getString(2));
//			}
//			pstate1.close();
			
			
			////////////////////////////////////
			long startTime = System.currentTimeMillis();
			List<Student> lists = new ArrayList<Student>();
			for (int i = 0; i < 300000; i++) {
				String id = UUID.randomUUID().toString();
				Student student = new Student(id, "xiaoming" + i, LocalDateTime.now()
						.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")), i%2==0?1:0, "beijing");
				lists.add(student);
			}
			bsUtil.saveEntityJdbcBatch(conn, lists);
			long endTime = System.currentTimeMillis();
			logger.debug("Operation database successfully ,consume time:"+(endTime - startTime));
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
