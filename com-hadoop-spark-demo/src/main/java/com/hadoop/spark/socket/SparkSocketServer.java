package com.hadoop.spark.socket;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * 实现一个Socket Server。在本地启动一个ServerSocket，端口号设为9999，启动后开始监听客户端连接，
 * 一旦连接成功，打印客户端地址，然后向客户端推送一串字符串，时间间隔为1秒钟，循环一百次。
 */
@Slf4j
public class SparkSocketServer {
    static ServerSocket serverSocket = null;
    static PrintWriter pw = null;
    private static int port = 9999;

    public static void main(String[] args) {
        try {
            serverSocket = new ServerSocket(port);
            log.info("---Socket 服务启动:{}，等待连接-----", serverSocket);
            Socket socket = serverSocket.accept();
            log.info("----连接成功，来自：" + socket.getRemoteSocketAddress());
            pw = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));
            int i = 0;
            while (i < 500) {
                i++;
                String str = "spark streaming test " + i;
                pw.println(str);
                pw.flush();
                log.info("-->服务端发送数据:{}", str);
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                pw.close();
                serverSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
