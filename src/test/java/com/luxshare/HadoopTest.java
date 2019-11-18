package com.luxshare;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * hadoop  java api 连接测试
 *
 * @author lion hua
 * @since 2019-11-18
 */
@Slf4j
public class HadoopTest {

    @Test
    public void test() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        final FileSystem fileSystem = FileSystem.get(new URI("hdfs://10.32.36.132:8200"), configuration, "root");

        Path path = new Path("/hadoop/api/test");
        final boolean b = fileSystem.mkdirs(path);
        log.info("创建文件夹是否成功:{}", b);

    }
}
