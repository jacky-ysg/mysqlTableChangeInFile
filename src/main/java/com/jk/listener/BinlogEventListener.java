package com.jk.listener;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import com.jk.TableChangeApplication;
import lombok.extern.slf4j.Slf4j;

import java.io.FileWriter;
import java.io.IOException;
/**
 * @Author Jacky
 * @Date 2023/9/28 17:00
 */
@Slf4j
public class BinlogEventListener implements BinaryLogClient.EventListener {
    private final FileWriter fileWriter;
    private final String targetSchema;
    private final BinaryLogClient client;

    private boolean runFlag;

    public BinlogEventListener(String filePath, String targetSchema,BinaryLogClient client,boolean runFlag) throws IOException {
        this.fileWriter = new FileWriter(filePath, true);
        this.targetSchema = targetSchema;
        this.client = client;
        this.runFlag = runFlag;
    }

    @Override
    public void onEvent(Event event) {
        EventType eventType = event.getHeader().getEventType();

        if (eventType == EventType.ROTATE) {
            RotateEventData rotateEventData = event.getData();
            String newBinlogFilename = rotateEventData.getBinlogFilename();
            long newBinlogPosition = rotateEventData.getBinlogPosition();

            log.info("监听到binlog文件发生变化 文件名为:{} 位置为:{}",newBinlogFilename,newBinlogPosition);
            // 更新BinaryLogClient的binlog文件名和位置
            client.setBinlogFilename(newBinlogFilename);
            client.setBinlogPosition(newBinlogPosition);

            // 将新的binlog位置写入文件
            TableChangeApplication.writeBinlogPosition(newBinlogPosition);
        }

        if (eventType == EventType.QUERY) {
            if (!runFlag){
                log.info("This position continue because system is restart");
                runFlag = true;
                return;
            }
            QueryEventData queryEventData = event.getData();
            String sql = queryEventData.getSql().toUpperCase();

            // 检测CREATE TABLE、ALTER TABLE和DROP TABLE语句，并且仅处理目标schema的操作
            if ((sql.contains("CREATE TABLE") || sql.contains("ALTER TABLE") || sql.contains("DROP TABLE"))
                    && queryEventData.getDatabase().equalsIgnoreCase(targetSchema))
                try {
                    log.info("监听到对应schema:{} 表结构发生变化sql语句为:{}",targetSchema,queryEventData.getSql());
                    fileWriter.write(queryEventData.getSql());
                    fileWriter.write(";" + "\n");
                    fileWriter.flush();
                } catch (IOException e) {
                    // 异常处理
                    log.error("sql文件写入异常:" + e);
                }finally {
                    TableChangeApplication.writeBinlogPosition(client.getBinlogPosition());
                }
        }
    }
}
