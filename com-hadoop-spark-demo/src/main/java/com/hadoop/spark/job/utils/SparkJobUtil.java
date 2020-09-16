package com.hadoop.spark.job.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;

public class SparkJobUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(SparkJobUtil.class);

    /**
     * Close quietly.
     *
     * @param fileSystem the file system
     */
    public static void closeQuietly(FileSystem fileSystem) {
        if (fileSystem != null) {
            try {
                fileSystem.close();
            } catch (IOException e) {
                LOGGER.error("------>Fail to close FileSystem:" + fileSystem, e);
            }
        }
    }

    /**
     * Check file exists.
     *
     * @param path the path
     */
    public static void checkFileExists(String path) {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(configuration);
            if (!fileSystem.exists(new Path(path))) {
                LOGGER.error("-----File:{} Not Found------", path);
                throw new FileNotFoundException(path);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            closeQuietly(fileSystem);
        }
    }
}
