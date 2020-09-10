package com.hadoop.hive.service;

import com.hadoop.hive.config.DataSourceProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * hadoop 下创建HDFS目录  ./hadoop fs -mkdir -p /eric/hadoop_data/input
 * hadoop 下查看HDFS目录下的文件列表 ./hadoop fs -ls /eric/hadoop_data/input
 * hadoop 下删除HDFS目录下的文件 ./hadoop fs -rm -r -skipTrash /eric/hadoop_data/input/word.txt
 * hadoop 下删除HDFS目录 ./hadoop fs -rm -r -skipTrash /eric/hadoop_data/input
 * hadoop 下从本地目录复制文件到HDFS目录 ./hadoop fs -put /home/hadoop_data/word.txt /eric/hadoop_data/input
 * 使用HDFS全路径访问 ./hadoop fs -ls hdfs://172.12.0.4:9000/
 * ./hadoop fs -ls hdfs://172.12.0.4:9000/
 * <p>
 * Found 4 items
 * drwxr-xr-x   - root supergroup          0 2020-09-07 02:41 hdfs://172.12.0.4:9000/eric
 * drwxr-xr-x   - root supergroup          0 2020-09-07 02:32 hdfs://172.12.0.4:9000/home
 * drwx-wx-wx   - root supergroup          0 2020-09-03 08:51 hdfs://172.12.0.4:9000/tmp
 * drwxr-xr-x   - root supergroup          0 2020-09-03 08:03 hdfs://172.12.0.4:9000/user
 * <p>
 * hadoop 列出在指定HDFS目录下的文件内容 ./hadoop fs -ls /eric/hadoop_data/input
 * Found 1 items
 * -rw-r--r--   1 root supergroup        814 2020-09-07 03:17 /eric/hadoop_data/input/word.txt
 */
@Slf4j
@Service
public class HadoopHDFSService {
    @Autowired
    private DataSourceProperties dataSourceProperties;

    /**
     * 获取HDFS配置信息
     *
     * @return
     */
    public Configuration getConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set(dataSourceProperties.getHdfs().get("name"), dataSourceProperties.getHdfs().get("url"));
        //设置通过域名访问datanode
        configuration.set("dfs.client.use.datanode.hostname", "true");
        return configuration;

    }

    /**
     * 获取HDFS文件系统对象
     *
     * @return
     * @throws Exception
     */
    public FileSystem getFileSystem() throws Exception {
        // 客户端去操作hdfs时是有一个用户身份的，默认情况下hdfs客户端api会从jvm中获取一个参数作为自己的用户身份
        // DHADOOP_USER_NAME=hadoop
        // 也可以在构造客户端fs对象时，通过参数传递进去
        return FileSystem.get(new URI(dataSourceProperties.getHdfs().get("url")), getConfiguration(), dataSourceProperties.getHdfs().get("user"));
    }

    /**
     * 在HDFS创建文件夹
     *
     * @param path
     * @return
     * @throws Exception
     */
    public boolean mkDir(String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return false;
        }
        if (existFile(path)) {
            return true;
        }
        FileSystem fs = getFileSystem();
        // 目标路径
        Path srcPath = new Path(path);
        boolean isOk = fs.mkdirs(srcPath);
        fs.close();
        log.info("---->在HDFS创建文件夹目录->{},操作{}!", path, isOk ? "成功" : "失败");
        return isOk;
    }

    /**
     * 判断HDFS文件是否存在
     *
     * @param path
     * @return
     * @throws Exception
     */
    public boolean existFile(String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return false;
        }
        FileSystem fs = getFileSystem();
        Path srcPath = new Path(path);
        return fs.exists(srcPath);
    }

    /**
     * 判断HDFS文件夹是否存在
     *
     * @param path
     * @return
     * @throws Exception
     */
    public boolean existDir(String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return false;
        }

        FileSystem fs = getFileSystem();
        Path srcPath = new Path(path);
        return fs.isDirectory(srcPath);
    }

    /**
     * 删除HDFS文件夹
     *
     * @param path
     * @return
     * @throws Exception
     */
    public boolean deleteDir(String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return false;
        }
        if (!existFile(path)) {
            return false;
        }

        FileSystem fs = getFileSystem();
        Path srcPath = new Path(path);
        boolean isOk = fs.delete(srcPath, true);
        fs.close();
        log.info("---->在HDFS删除文件夹目录->{},操作{}!", path, isOk ? "成功" : "失败");
        return isOk;
    }

    /**
     * 读取HDFS目录信息
     *
     * @param path
     * @return
     * @throws Exception
     */
    public List<Map<String, Object>> readPathInfo(String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return null;
        }
        if (!existFile(path)) {
            return null;
        }
        FileSystem fs = getFileSystem();
        // 目标路径
        Path newPath = new Path(path);
        FileStatus[] statusList = fs.listStatus(newPath);
        ContentSummary contentSummary = fs.getContentSummary(newPath);
        //集群占用空间, 一般来说是实际占用空间的几倍, 具体与配置的副本数相关.
        long clusterSpace = contentSummary.getSpaceConsumed();
        //实际占用空间
        long actualSpace = contentSummary.getLength();
        log.info("集群占用空间->{},实际占用空间->{}", clusterSpace, actualSpace);
        List<Map<String, Object>> list = new ArrayList<>();
        if (null != statusList && statusList.length > 0) {
            for (FileStatus file : statusList) {
                Map<String, Object> map = new HashMap<>();
                map.put("filePath", file.getPath());
                map.put("fileStatus", file.toString());
                list.add(map);
            }
            return list;
        } else {
            return null;
        }
    }

    /**
     * @param path 显示HDFS目录及文件列表
     */
    public void showFiles(String path) {
        FileSystem fileSystem = null;
        try {
            fileSystem = this.getFileSystem();
            Path newPath = new Path(path);

            iteratorShowFiles(fileSystem, newPath);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (fileSystem != null) {
                try {
                    fileSystem.closeAll();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 遍历HDFS目录
     *
     * @param fileSystem FileSystem 对象
     * @param path       文件路径
     */
    public void iteratorShowFiles(FileSystem fileSystem, Path path) {
        try {
            if (fileSystem == null || path == null) {
                return;
            }
            //获取文件列表
            FileStatus[] files = fileSystem.listStatus(path);
            ContentSummary contentSummary = fileSystem.getContentSummary(path);
            //集群占用空间, 一般来说是实际占用空间的几倍, 具体与配置的副本数相关.
            long clusterSpace = contentSummary.getSpaceConsumed();
            //实际占用空间
            long actualSpace = contentSummary.getLength();

            log.info("集群占用空间->{},实际占用空间->{}", clusterSpace, actualSpace);
            //展示文件信息
            for (FileStatus file : files) {
                try {
                    if (file.isDirectory()) {
                        log.info("路径" + file.getPath()
                                + ",路径权限用户:" + file.getOwner());
                        //递归调用
                        iteratorShowFiles(fileSystem, file.getPath());
                    } else if (file.isFile()) {
                        log.info("文件路径" + file.getPath()
                                + ",文件大小:" + file.getLen()
                                + ",文件权限用户:" + file.getOwner());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * HDFS创建文件
     *
     * @param path
     * @param file
     * @throws Exception
     */
    public void createFile(String path, MultipartFile file) throws Exception {
        if (StringUtils.isEmpty(path) || null == file.getBytes()) {
            return;
        }
        String fileName = file.getOriginalFilename();
        FileSystem fs = getFileSystem();
        // 上传时默认当前目录，后面自动拼接文件的目录
        Path newPath = new Path(path + "/" + fileName);
        // 打开一个输出流
        FSDataOutputStream outputStream = fs.create(newPath);
        outputStream.write(file.getBytes());
        outputStream.close();
        fs.close();
    }

    /**
     * 读取HDFS文件内容
     *
     * @param path
     * @return
     * @throws Exception
     */
    public String readFile(String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return null;
        }
        if (!existFile(path)) {
            return null;
        }
        FileSystem fs = getFileSystem();
        // 目标路径
        Path srcPath = new Path(path);
        FSDataInputStream inputStream = null;
        try {
            inputStream = fs.open(srcPath);
            // 防止中文乱码
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String lineTxt;
            StringBuilder sb = new StringBuilder();
            while ((lineTxt = reader.readLine()) != null) {
                sb.append(lineTxt);
            }
            return sb.toString();
        } finally {
            assert inputStream != null;
            inputStream.close();
            fs.close();
        }
    }

    /**
     * 读取HDFS文件列表
     *
     * @param path
     * @return
     * @throws Exception
     */
    public List<Map<String, String>> listFile(String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return null;
        }
        if (!existFile(path)) {
            return null;
        }

        FileSystem fs = getFileSystem();
        // 目标路径
        Path srcPath = new Path(path);
        // 递归找到所有文件
        RemoteIterator<LocatedFileStatus> filesList = fs.listFiles(srcPath, true);
        List<Map<String, String>> returnList = new ArrayList<>();
        while (filesList.hasNext()) {
            LocatedFileStatus next = filesList.next();
            String fileName = next.getPath().getName();
            Path filePath = next.getPath();
            Map<String, String> map = new HashMap<>();
            map.put("fileName", fileName);
            map.put("filePath", filePath.toString());
            returnList.add(map);
        }
        fs.close();
        return returnList;
    }

    /**
     * HDFS重命名文件
     *
     * @param oldName
     * @param newName
     * @return
     * @throws Exception
     */
    public boolean renameFile(String oldName, String newName) throws Exception {
        if (StringUtils.isEmpty(oldName) || StringUtils.isEmpty(newName)) {
            return false;
        }
        FileSystem fs = getFileSystem();
        // 原文件目标路径
        Path oldPath = new Path(oldName);
        // 重命名目标路径
        Path newPath = new Path(newName);
        boolean isOk = fs.rename(oldPath, newPath);
        fs.close();
        log.info("---->在HDFS重命名文件->{},操作{}!", oldName, isOk ? "成功" : "失败");
        return isOk;
    }

    /**
     * 删除HDFS文件
     *
     * @param path
     * @return
     * @throws Exception
     */
    public boolean deleteFile(String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return false;
        }
        if (!existFile(path)) {
            return false;
        }
        FileSystem fs = getFileSystem();
        Path srcPath = new Path(path);
        boolean isOk = fs.deleteOnExit(srcPath);
        fs.close();
        log.info("---->在HDFS删除文件->{},操作{}!", path, isOk ? "成功" : "失败");
        return isOk;
    }

    /**
     * 上传HDFS文件
     *
     * @param path
     * @param uploadPath
     * @throws Exception
     */
    public void uploadFile(String path, String uploadPath) throws Exception {
        if (StringUtils.isEmpty(path) || StringUtils.isEmpty(uploadPath)) {
            return;
        }
        FileSystem fs = getFileSystem();
        // 上传路径
        Path clientPath = new Path(path);
        // 目标路径
        Path serverPath = new Path(uploadPath);

        // 调用文件系统的文件复制方法，第一个参数是否删除原文件true为删除，默认为false
        fs.copyFromLocalFile(false, clientPath, serverPath);
        fs.close();
        log.info("---->在HDFS上传文件->{},到{}操作完成!", path, uploadPath);
    }

    /**
     * 下载HDFS文件
     *
     * @param path
     * @param downloadPath
     * @throws Exception
     */
    public void downloadFile(String path, String downloadPath) throws Exception {
        if (StringUtils.isEmpty(path) || StringUtils.isEmpty(downloadPath)) {
            return;
        }
        FileSystem fs = getFileSystem();
        // 上传路径
        Path clientPath = new Path(path);
        // 目标路径
        Path serverPath = new Path(downloadPath);

        // 调用文件系统的文件复制方法，第一个参数是否删除原文件true为删除，默认为false
        fs.copyToLocalFile(false, clientPath, serverPath);
        fs.close();
        log.info("---->在HDFS下载文件->{},到{}操作完成!", path, downloadPath);
    }

    /**
     * HDFS文件复制
     *
     * @param sourcePath
     * @param targetPath
     * @throws Exception
     */
    public void copyFile(String sourcePath, String targetPath) throws Exception {
        if (StringUtils.isEmpty(sourcePath) || StringUtils.isEmpty(targetPath)) {
            return;
        }
        FileSystem fs = getFileSystem();
        // 原始文件路径
        Path oldPath = new Path(sourcePath);
        // 目标路径
        Path newPath = new Path(targetPath);

        FSDataInputStream inputStream = null;
        FSDataOutputStream outputStream = null;
        try {
            inputStream = fs.open(oldPath);
            outputStream = fs.create(newPath);

            int bufferSize = 1024 * 1024 * 64;
            IOUtils.copyBytes(inputStream, outputStream, bufferSize, false);
        } finally {
            assert inputStream != null;
            inputStream.close();
            assert outputStream != null;
            outputStream.close();
            fs.close();
        }
    }

    /**
     * 获取某个文件在HDFS的集群位置
     *
     * @param path
     * @return
     * @throws Exception
     */
    public BlockLocation[] getFileBlockLocations(String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return null;
        }
        if (!existFile(path)) {
            return null;
        }
        FileSystem fs = getFileSystem();
        // 目标路径
        Path srcPath = new Path(path);
        FileStatus fileStatus = fs.getFileStatus(srcPath);
        return fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
    }
}
