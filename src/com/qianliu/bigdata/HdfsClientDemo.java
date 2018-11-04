package com.qianliu.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class HdfsClientDemo {
    FileSystem fileSystem = null;
    @Before
    public void init() throws Exception {
        //System.setProperty("hadoop.home.dir", "E:\\hadoopConfig\\hadoop-common-2.2.0-bin-master");//因为win中没有shell的执行能力，这里有一个外接的软件来使用shell

        //配置configuration是为了告诉程序使用fs.defaultFS这种文件格式在hdfs://192.168.48.136:9000上操作
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://192.168.48.136:9000");

        //参数1：hdfs://192.168.48.136:9000是连接的对象，参数3：表示用"root"身份登陆
        fileSystem = FileSystem.get(new URI("hdfs://192.168.48.136:9000"),configuration,"root");
    }

    @Test
    public void testUpLoad() throws IOException {
        fileSystem.copyFromLocalFile(new Path("./fileTest/1.txt"),new Path("/1.txt.copy"));
        fileSystem.close();
    }
}
