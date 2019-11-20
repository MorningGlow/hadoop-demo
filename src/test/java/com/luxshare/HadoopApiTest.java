package com.luxshare;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * hadoop java api 测试 (与 hadoop shell 命令相对应)
 *
 * @author lion hua
 * @since 2019-11-19
 */
@Slf4j
public class HadoopApiTest {


    public static final String HDFS_10_32_36_132_8200 = "hdfs://10.32.36.132:8200";
    public static final String ROOT = "root";
    private Configuration configuration = null;

    private FileSystem fileSystem = null;

    @Before
    public void before() throws URISyntaxException, IOException, InterruptedException {
        configuration = new Configuration();
        // 设置副本系数 默认为 3 , 修改为 1
        configuration.set("dfs.replication", "1");
        fileSystem = FileSystem.get(new URI(HDFS_10_32_36_132_8200), configuration, ROOT);
    }

    /**
     * 创建一个文件夹
     */
    @Test
    public void test() throws IOException {
        boolean b = fileSystem.mkdirs(new Path("/hadoop/java/test"));
        log.info("文件创建:{}", b);
    }

    /**
     * 删除指定的目录
     *
     * @throws IOException exception
     */
    @Test
    public void test2() throws IOException {
        boolean b = fileSystem.delete(new Path("/hadoop"), true);
        log.info("删除:{}", b);
    }

    /**
     * 读取指定文件中的内容
     */
    @Test
    public void test3() throws IOException {

        FSDataInputStream in = fileSystem.open(new Path("/remark.txt"), 512);

        IOUtils.copyBytes(in, System.out, 1024);
    }

    /**
     * 创建一个文件,并写入内容
     */
    @Test
    public void test4() throws IOException {
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/hadoop/java/test/java.txt"));
        // 写入数据
        fsDataOutputStream.writeUTF("大数据啊，big data!");
        // 防止没有真的写出
        fsDataOutputStream.flush();
        fsDataOutputStream.close();
    }

    /**
     * 重命名
     */
    @Test
    public void test5() throws IOException {
        boolean b = fileSystem.rename(new Path("/hadoop/java/test/java.txt"), new Path("/hadoop/java/test/111.txt"));
        log.info("重命名:{}", b);
    }

    /**
     * 从本地上传文件
     */
    @Test
    public void test6() throws IOException {
        fileSystem.copyFromLocalFile(new Path("C:\\Users\\12755167\\Desktop\\安装包\\jdk-8u191-linux-x64.tar.gz"), new Path("/hadoop/java/test/java.tar.gz"));
    }

    /**
     * 带进度条上传文件
     */
    @Test
    public void test7() throws IOException {
        InputStream in = new FileInputStream("C:\\Users\\12755167\\Desktop\\安装包\\jdk-8u191-linux-x64.tar.gz");

        FSDataOutputStream out = fileSystem.create(new Path("/hadoop/java/test/2222.tar.gz"), new Progressable() {
            @Override
            public void progress() {
                log.info("#");
            }
        });

        IOUtils.copyBytes(in, out, 10240);

    }

    /**
     * 从 hadoop 上下载大文件
     */
    @Test
    public void test8() throws IOException {
        // 报错(报指针异常)
//        fileSystem.copyToLocalFile(new Path("/hadoop/java/test/2222.tar.gz"),new Path("C:\\Users\\12755167\\Desktop\\安装包"));
        fileSystem.copyToLocalFile(false, new Path("/hadoop/java/test/2222.tar.gz"), new Path("C:\\Users\\12755167\\Desktop\\安装包"), true);

    }


    @After
    public void after() throws IOException {
        fileSystem.close();
        fileSystem = null;
        configuration = null;
    }

}
