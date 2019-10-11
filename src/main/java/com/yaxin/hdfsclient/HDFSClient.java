package com.yaxin.hdfsclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

public class HDFSClient {

    private FileSystem fs = null;

    //**关于什么时候在方法抛异常,还是在该方法try/catch的思路:当你给别人写方法,就抛异常;当你用别人的方法就处理异常.

    @Before
    public void before() throws IOException, InterruptedException {
        System.out.println("Before!!!!!!!!!!!!!");
        fs = FileSystem.get(URI.create("hdfs://hadoop102:9000"),
                new Configuration(), "atguigu");
    }

    /**
     * 单元测试如果正确的话exit code为0
     *
     * @throws IOException
     */
    @Test
    public void getOrPut() throws IOException, InterruptedException {
        //Configuration就是我们在hadoop2.7.2/etc/hadoop/*.xml文件中的configuration,'name'就是key,'value'就是value
        //这里没有传配置就会使用默认的配置文件,默认配置在jar包中,固定的
        Configuration configuration = new Configuration();
        //1.获取一个HDFS的抽象封装对象,9000是hdfs的ipc端口号
        FileSystem fs = FileSystem.get(URI.create("hdfs://hadoop102:9000"),
                configuration, "atguigu");
        //2.用这个对象操作文件系统
        //2.1上传到hdfs
        fs.copyFromLocalFile(new Path("D:\\1.txt"), new Path("/"));
        //2.2下载到本地文件系统
//        fs.copyToLocalFile(new Path("/wcoutput"),new Path("D:\\"));

        //2.3hdfs不支持并发操作,这里必须要关闭!
        fs.close();

    }

    /**
     * 重命名
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void rename() throws IOException, InterruptedException {
        fs.rename(new Path("/1.txt"), new Path("/2.txt"));
    }


    /**
     * 删除文件/文件夹(支持递归删除)
     * recursive就是我们在命令行的-r
     *
     * @throws IOException
     */
    @Test
    public void delete() throws IOException {
        boolean delete = fs.delete(new Path("/1.txt"), true);
        if (delete) {
            System.out.println("删除成功");
        } else {
            System.out.println("删除失败");
        }
    }

    /**
     * 从本地文件追加信息到hdfs
     *
     * @throws IOException
     */
    @Test
    public void du() throws IOException {
        FSDataOutputStream append = fs.append(new Path("/test/1.txt"), 1024);
        FileInputStream open = new FileInputStream("d:\\1.txt");
        //Apache提供的流包的工具类
        IOUtils.copyBytes(open, append, 1024, true);

    }

    /**
     * @throws IOException
     */
    @Test
    public void ls() throws IOException {
        //ls的全称就是listStatus
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus: fileStatuses) {
            if(fileStatus.isFile()){
                System.out.println("以下信息是一个文件信息");
                System.out.println("文件路径:"+fileStatus.getPath());
                System.out.println("文件长度:"+fileStatus.getLen());
                System.out.println("文件权限:"+fileStatus.getPermission());
                System.out.println("文件块大小:"+fileStatus.getBlockSize());
                System.out.println("文件所属者:"+fileStatus.getOwner());
                System.out.println("文件副本数:"+fileStatus.getReplication());
                System.out.println("文件通过时间:"+fileStatus.getAccessTime());
                System.out.println("----------------------");
            }else if(fileStatus.isDirectory()){
                System.out.println("这是一个文件夹信息!");
                System.out.println(fileStatus.getPath());
                System.out.println("----------------------");
            }else if(fileStatus.isSymlink()){
                //可能有软链接,还要单独判断
                System.out.println("这是一个软链接!");
                System.out.println(fileStatus.getPath());
                System.out.println("----------------------");
            }else {

            }
        }
    }

    @After
    public void after() throws IOException {
        System.out.println("After!!!!!!");
        fs.close();
    }
}
