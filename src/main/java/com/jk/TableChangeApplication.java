package com.jk;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.jk.listener.BinlogEventListener;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.*;
import java.sql.*;
/**
 * @Author Jacky
 * @Date 2023/9/28 16:51
 */
@SpringBootApplication
@Slf4j
public class TableChangeApplication implements ApplicationRunner {

    // 存储binlog标记的文件
    private static final String BINLOG_POSITION_FILE = "binlog_position.txt";
    // 存储变更结构的的sql文件
    private static final String SAVE_CHANGE_SQL_FILE = "tableChange.sql";
    // 默认的binlog文件
    private static final String MYSQL_DEFAULT_BIN_LOG_FILE = "mysql-bin.000001";
    // mysql驱动url
    private static final String MYSQL_URL = "jdbc:mysql://localhost:3306";
    // mysql主机
    private static final String MYSQL_HOST = "localhost";
    // mysql端口号
    private static final Integer MYSQL_PORT = 3306;
    // mysql用户名
    private static final String MYSQL_UNAME = "root";
    // mysql密码
    private static final String MYSQL_PWD = "root";
    // 要监听的schema
    private static final String MYSQL_SCHEMA = "xxxx";

    private static boolean runFlag = false;


    public static void main(String[] args) {
        SpringApplication.run(TableChangeApplication.class,args);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        // 检查是否存在标记文件
        long startPosition = readBinlogPosition();

        BinaryLogClient client = new BinaryLogClient(MYSQL_HOST, MYSQL_PORT, MYSQL_UNAME, MYSQL_PWD);
        client.registerEventListener(new BinlogEventListener(SAVE_CHANGE_SQL_FILE,MYSQL_SCHEMA,client,runFlag));

        if (startPosition > 0) {
            client.setBinlogFilename(getLatestBinlogFilename()); // 设置起始binlog文件名
            client.setBinlogPosition(startPosition); // 设置起始标记
        }

        try {
            client.connect();
        } catch (IOException e) {
            // 处理连接错误
            log.error("binlog监听系统连接异常:" + e);
        }

    }


    private static long readBinlogPosition() {
        File file = new File(BINLOG_POSITION_FILE);
        if (file.exists()) {
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String positionString = reader.readLine();
                return Long.parseLong(positionString);
            } catch (IOException e) {
                log.error("读取binlog位置文件异常:" + e);
            }
        }
        runFlag = true;
        return 0;
    }

    public static void writeBinlogPosition(long position) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(BINLOG_POSITION_FILE))) {
            writer.write(String.valueOf(position));
        } catch (IOException e) {
            log.error("写入binlog位置文件异常:" + e);
        }
    }

    private static String getLatestBinlogFilename() {

        try (Connection connection = DriverManager.getConnection(MYSQL_URL, MYSQL_UNAME, MYSQL_PWD);
             Statement statement = connection.createStatement()) {
            String query = "SHOW MASTER STATUS";
            ResultSet resultSet = statement.executeQuery(query);

            if (resultSet.next()) {
                return resultSet.getString("File");
            }
        } catch (SQLException e) {
            log.error("获取最新的binlog文件名时发生异常：" + e.getMessage());
        }

        return MYSQL_DEFAULT_BIN_LOG_FILE;
    }
}
