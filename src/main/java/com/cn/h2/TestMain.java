package com.cn.h2;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTextField;

/**
 * 
 * @author xch
 *
 */
public class TestMain extends JFrame implements ActionListener {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Connection conn = null; //
	private JButton btn_createConn, btn_closeConn, btn_createTable, btn_dropTable, btn_insert, btn_insert_batch, btn_update, btn_update2, btn_delete, btn_query; //
	private JTextField textField_id; //

	public TestMain() {
		this.setTitle("内存数据库测试【必须先创建连接才能进行其他操作】"); //
		this.setSize(1200, 100);
		this.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE); //

		btn_createConn = new JButton("创建连接");
		btn_closeConn = new JButton("关闭连接"); //
		btn_createTable = new JButton("建表");
		btn_dropTable = new JButton("删表");
		btn_insert = new JButton("插入数据");
		btn_insert_batch = new JButton("批量插入");
		btn_update = new JButton("遍历修改");
		btn_update2 = new JButton("遍历修改2");
		btn_delete = new JButton("删除数据");
		btn_query = new JButton("查询数据");

		btn_createConn.addActionListener(this); //
		btn_closeConn.addActionListener(this); //
		btn_createTable.addActionListener(this); //
		btn_dropTable.addActionListener(this); //
		btn_insert.addActionListener(this); //
		btn_insert_batch.addActionListener(this); //
		btn_update.addActionListener(this); //
		btn_update2.addActionListener(this); //
		btn_delete.addActionListener(this); //
		btn_query.addActionListener(this); //

		textField_id = new JTextField("4612%");
		textField_id.setPreferredSize(new Dimension(100, 25));

		JPanel btn_panel = new JPanel(new FlowLayout(FlowLayout.LEFT)); //
		btn_panel.add(btn_createConn); //
		btn_panel.add(btn_closeConn); //
		btn_panel.add(btn_createTable); //
		btn_panel.add(btn_dropTable); //
		btn_panel.add(btn_insert); //
		btn_panel.add(btn_insert_batch); //
		btn_panel.add(btn_update); //
		btn_panel.add(btn_update2); //
		btn_panel.add(btn_delete); //
		btn_panel.add(btn_query); //
		btn_panel.add(textField_id); //

		this.getContentPane().add(btn_panel, BorderLayout.NORTH); //
		this.setVisible(true);
	}

	private void createConnection() throws Exception {
		if (conn != null) {
			System.err.println("连接已经创建好了！无需重复创建。。。"); //
			return;
		}
		long ll_1 = System.currentTimeMillis();
		//Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
		//conn = DriverManager.getConnection("jdbc:derby:MyTestDB;create=true"); //在JVM直接创建本地连接,相当于Loj4j访问本地日志文件,理论上速度是最快的.

		//Class.forName("org.apache.derby.jdbc.ClientDriver");
		//conn = DriverManager.getConnection("jdbc:derby://127.0.0.1:1529/XchDB;create=true"); //

		Class.forName("org.h2.Driver");
		//		conn = DriverManager.getConnection("jdbc:h2:tcp://localhost:9092/./OKDB"); //网络通讯,服务器端是文件!
		conn = DriverManager.getConnection("jdbc:h2:tcp://127.0.0.1:9092/mem:test"); //网络通讯,服务器端是内存

		//conn = DriverManager.getConnection("jdbc:h2:mem:XchDB;DB_CLOSE_DELAY=-1;CACHE_SIZE=102400");  //本地内存

		//String str_dbName = UUID.randomUUID().toString(); //每次都创建一个新的唯一的库!因一为个库就是一个文件，一个线程访问一个文件最快！
		//conn = DriverManager.getConnection("jdbc:h2:file:K:/555/" + str_dbName + ";AUTO_SERVER=FALSE;CACHE_SIZE=102400"); //本地文件

		//conn.setAutoCommit(true);
		long ll_2 = System.currentTimeMillis(); //
		System.out.println("创建数据库并连接成功,耗时【" + (ll_2 - ll_1) + "】"); //【" + str_dbName + "】
	}

	private void closeConnection() {
		try {
			long ll_1 = System.currentTimeMillis();
			conn.close(); //
			conn = null; //
			long ll_2 = System.currentTimeMillis(); //
			System.out.println("关闭数据库连接成功,耗时【" + (ll_2 - ll_1) + "】"); //
		} catch (Exception _ex) {
			_ex.printStackTrace(); //
		}
	}

	//建表..
	private void onCreateTable() throws Exception {
		long ll_1 = System.currentTimeMillis();
		Statement stmt1 = conn.createStatement(); //
		stmt1.executeUpdate("create table testtable(id varchar(50),name varchar(128))");
		stmt1.executeUpdate("create index in_testtable_1 on testtable(id)");
		stmt1.close();
		long ll_2 = System.currentTimeMillis(); //
		System.out.println("建表成功,耗时【" + (ll_2 - ll_1) + "】"); //
	}

	//删表
	private void onDropTable() throws Exception {
		long ll_1 = System.currentTimeMillis();
		Statement stmt = conn.createStatement(); //
		stmt.executeUpdate("drop table testtable");
		conn.commit();
		stmt.close();
		long ll_2 = System.currentTimeMillis(); //
		System.out.println("删表成功,耗时【" + (ll_2 - ll_1) + "】"); //
	}

	private void onQuery() throws Exception {
		String str_id = textField_id.getText(); //
		long ll_1 = System.currentTimeMillis();
		Statement stmt = conn.createStatement(); //
		String str_sql = "select * from testtable where id ";
		if (str_id.endsWith("%")) {
			str_sql = str_sql + "like '" + str_id + "'";
		} else {
			str_sql = str_sql + "= '" + str_id + "'";
		}
		System.err.println("查询SQL【" + str_sql + "】"); //
		ResultSet rs = stmt.executeQuery(str_sql); //
		while (rs.next()) {
			System.out.println("id【" + rs.getString("id") + "】name【" + rs.getString("name") + "】");
		}
		stmt.close();
		long ll_2 = System.currentTimeMillis(); //
		System.out.println("查询数据耗时【" + (ll_2 - ll_1) + "】"); //
	}

	//一条条插入
	private void onInsert() throws Exception {
		long ll_1 = System.currentTimeMillis();
		Statement stmt = conn.createStatement(); //
		int li_count = 100000; //插入1万条数据是20秒左右,3万条是1分钟左右
		for (int i = 1; i <= li_count; i++) {
			stmt.executeUpdate("insert into testtable (id,name) values ('" + i + "','老徐到此一游" + i + "')");
			//conn.commit();
		}
		stmt.close();
		long ll_2 = System.currentTimeMillis(); //
		System.out.println("插入[" + li_count + "]条数据耗时【" + (ll_2 - ll_1) + "】"); //
	}

	//批量插入
	private void onInsertBatch() throws Exception {
		long ll_1 = System.currentTimeMillis();
		int li_count = 300000; //插入1万条数据是20秒左右,3万条是1分钟左右
		ArrayList<String> sqlList = new ArrayList<String>();
		for (int i = 1; i <= li_count; i++) {
			String str_sql = "insert into testtable (id,name) values ('" + i + "','老徐到此一游" + i + "')"; //
			sqlList.add(str_sql); //
			if (sqlList.size() >= 3000) {
				doBatch(sqlList); //
			}
		}
		doBatch(sqlList); //
		long ll_2 = System.currentTimeMillis(); //
		System.out.println("批量插入[" + li_count + "]条数据耗时【" + (ll_2 - ll_1) + "】"); //
	}

	//全量数据遍历循环修改
	private void onUpdate() throws Exception {
		long ll_1 = System.currentTimeMillis();
		ArrayList<String> sqlList = new ArrayList<String>();
		Statement stmt = conn.createStatement(); //
		ResultSet rs = stmt.executeQuery("select * from testtable"); //
		while (rs.next()) {
			String str_id = rs.getString("id"); //
			String str_name = rs.getString("name"); //
			String str_sql = "update testtable set name='" + str_name + "-New' where id = '" + str_id + "'"; //
			sqlList.add(str_sql); //
			if (sqlList.size() >= 1000) {
				doBatch(sqlList); //
			}
		}
		doBatch(sqlList); //
		stmt.close();
		long ll_2 = System.currentTimeMillis(); //
		System.out.println("遍历修改全量数据耗时【" + (ll_2 - ll_1) + "】"); //
	}

	private void onUpdate2() throws Exception {
		long ll_1 = System.currentTimeMillis();
		Statement stmt = conn.createStatement(); //
		ResultSet rs = stmt.executeQuery("select * from testtable"); //
		PreparedStatement updateStmt = conn.prepareStatement("update testtable set name=? where id=?"); //预编译的stmt
		int li_count = 0;
		while (rs.next()) {
			String str_id = rs.getString("id"); //
			String str_name = rs.getString("name"); //
			str_name = str_name + "-New"; //
			updateStmt.setString(1, str_name); //
			updateStmt.setString(2, str_id); //
			updateStmt.execute(); //执行!
			li_count++;
		}
		updateStmt.close();
		stmt.close();
		long ll_2 = System.currentTimeMillis(); //
		System.out.println("遍历修改全量数据【" + li_count + "】耗时【" + (ll_2 - ll_1) + "】"); //
	}

	//提交一批数据
	private void doBatch(ArrayList<String> _sqlList) throws Exception {
		int li_size = _sqlList.size();
		if (li_size <= 0) {
			return; //
		}
		long ll_1 = System.currentTimeMillis();
		Statement stmt = conn.createStatement(); //
		for (int i = 0; i < li_size; i++) {
			stmt.addBatch(_sqlList.get(i));
		}
		stmt.executeBatch(); //批量执行..
		conn.commit();
		stmt.close();
		_sqlList.clear(); //一定要清空
		long ll_2 = System.currentTimeMillis(); //
		System.err.println("批量执行[" + li_size + "]条SQL,耗时[" + (ll_2 - ll_1) + "]"); //
	}

	private void onDelete() throws Exception {
		long ll_1 = System.currentTimeMillis();
		Statement stmt = conn.createStatement(); //
		stmt.executeUpdate("truncate table testtable");
		conn.commit();
		stmt.close();
		long ll_2 = System.currentTimeMillis(); //
		System.out.println("删除数据成功,耗时【" + (ll_2 - ll_1) + "】"); //
	}

	@Override
	public void actionPerformed(ActionEvent _evt) {
		try {
			if (_evt.getSource() == btn_createConn) {
				createConnection(); //
			} else if (_evt.getSource() == btn_closeConn) {
				closeConnection(); //
			} else {
				if (conn == null) {
					JOptionPane.showMessageDialog(this, "必须先创建数据库连接!!"); //
					return;
				}
				if (_evt.getSource() == btn_createTable) {
					onCreateTable(); //
				} else if (_evt.getSource() == btn_dropTable) {
					onDropTable(); //
				} else if (_evt.getSource() == btn_insert) {
					onInsert(); //
				} else if (_evt.getSource() == btn_insert_batch) {
					onInsertBatch(); //
				} else if (_evt.getSource() == btn_update) {
					onUpdate(); //
				} else if (_evt.getSource() == btn_update2) {
					onUpdate2(); //
				} else if (_evt.getSource() == btn_delete) {
					onDelete(); //
				} else if (_evt.getSource() == btn_query) {
					onQuery(); //
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		new TestMain();
	}

}
