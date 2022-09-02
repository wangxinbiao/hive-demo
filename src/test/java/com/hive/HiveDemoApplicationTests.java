package com.hive;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.sql.*;

@SpringBootTest
class HiveDemoApplicationTests {

    @Autowired
    @Qualifier("hiveDruidDataSource")
    private DataSource druidDataSource;

    @Autowired
    private JdbcTemplate hiveDruidTemplate;

    @Test
    public void test_createTable() {
        hiveDruidTemplate.execute(sqlOfUserTableCreation());
    }

    @Test
    public void test_connect() {
        System.out.println(getData(druidDataSource, "select * from test", null));
        System.out.println(getData((druidDataSource), sqlOfUserTableCreation(), null));
    }

    public static String getData(DataSource druidDataSource, String sqlString, String resultName) {
        Connection conn = null;
        Statement statement = null;
        ResultSet rs = null;
        JSONObject result = null;
        try {
            conn = druidDataSource.getConnection();
            statement = conn.createStatement();
            result = new JSONObject();
            result.put("state", "0");
            JSONArray array = new JSONArray();
            rs = statement.executeQuery(sqlString);
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (rs.next()) {
                JSONObject jsonObj = new JSONObject();
                // 遍历每一列
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnLabel(i);
                    String value = rs.getString(columnName);
                    jsonObj.put(columnName, value);
                }
                array.add(jsonObj);
            }
            result.put("analysisMACResult", array.toString());
        } catch (SQLException e) {
            e.printStackTrace();
            return "{\"state\":\"1\"}";
        } finally {
            try {
                conn.close();
                statement.close();
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
                return "{\"state\":\"1\"}";
            }
        }
        return result.toString();
    }

    private String sqlOfUserTableCreation() {
        String sql = "create table if not exists `user` " +
                "(" +
                "  id bigint comment '唯一编号'" +
                "  ,name string comment '名称'" +
                "  ,birth string comment '生日'" +
                "  ,province string comment '籍贯'" +
                "  ,sex string comment '性别'" +
                ")" +
                " clustered by (province) into 3 buckets";
        return sql;
    }
}
