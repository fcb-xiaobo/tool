package com.dobest.sync.util;

import com.dobest.sync.config.StarRocksConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class StarRocksJdbcUtil {

    private Connection connection;

    @Autowired
    private StarRocksConfig starRocksConfig;

    private long lastId=0;

    @PostConstruct
    public void initConnection() {
        // 数据库的连接信息
        String url = starRocksConfig.getSrJdbcUrl();
        String user = starRocksConfig.getUserName();
        String password = starRocksConfig.getPassword();
        // 1. 加载数据库驱动
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            String printS = String.format("get jdbc connection fail,exception [ %s ]", e);
            throw new RuntimeException(printS);
        }
    }

    public void streamQuery(String table, int size,ResultSetHandler handler) {
        try {
            connection.setAutoCommit(false); // 关闭自动提交提升性能
            while (true) {
                String sql = String.format(
                        "SELECT * FROM %s WHERE iid > ? ORDER BY iid ASC LIMIT ?",
                        table);
                try (PreparedStatement stmt = connection.prepareStatement(sql,
                        ResultSet.TYPE_FORWARD_ONLY,
                        ResultSet.CONCUR_READ_ONLY)) {

                    stmt.setLong(1, lastId);
                    stmt.setInt(2, size);
                    stmt.setFetchSize(size); // 流式读取配置

                    ResultSet rs = stmt.executeQuery();
                    int batchCount = 0;

                    while (rs.next()) {
                        lastId = rs.getLong("iid"); // 更新当前批次最大ID
                        handler.processRow(rs); // 处理单行数据
                        batchCount++;
                    }
                    if (batchCount < size) break; // 最后一页退出循环
                }
                connection.commit(); // 显式提交当前批次
            }
        } catch (Exception e) {
            System.out.println("get search error,exception "+e.getMessage());
            System.out.println("stack into  "+e.getStackTrace());
            e.printStackTrace();

        }
    }

    @FunctionalInterface
    public interface ResultSetHandler {
        void processRow(ResultSet rs) throws SQLException;
    }
}


