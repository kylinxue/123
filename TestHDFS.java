import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

public class TestHDFS {

    /**
     * 通过URL的方法比较麻烦
     */
    @Test
    public void readFileByUrl() throws IOException {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        URL url = new URL("hdfs://192.168.204.201:8020/user/centos/hadoop/a.txt");
        URLConnection conn = url.openConnection();
        InputStream is = conn.getInputStream();
        byte[] buf = new byte[is.available()];
        is.read(buf);
        is.close();
        System.out.println(new String(buf));
    }

    /**
     * 通过Hadoop API访问
     *
     * @throws IOException
     */
    @Test
    public void readFile() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream fis = fs.open(new Path("/user/centos/hadoop/a.txt"));
        byte[] buf = new byte[1024];
        int len = -1;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        while ((len = fis.read(buf)) != -1) {
            baos.write(buf);
        }
        fis.close();
        baos.close();

        System.out.println(new String(baos.toByteArray()));
    }

    @Test
    public void readFileByIOUtils() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream fis = fs.open(new Path("/user/centos/hadoop/a.txt"));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IOUtils.copyBytes(fis, baos, 1024);
        System.out.println(new String(baos.toByteArray()));

    }

    @Test
    public void write() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        //权限不够，会失败
        FSDataOutputStream fout = fs.create(new Path("e:/a.txt"));
        fout.write("how are you".getBytes());
        fout.close();
    }

    @Test
    public void mkdirs() throws IOException {
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://s201:8020/");
        FileSystem fs = FileSystem.get(conf);
        fs.mkdirs(new Path("e:/hadooptest"));
    }

    /**
     * delete file and directory
     *
     * @throws IOException
     */
    @Test
    public void deletePath() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://s201:8020/");
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path("/user/centos/dir"), true);
    }

    /**
     * 创建文件或者将内容追加到已有文件中
     * 注意权限问题
     */
    @Test
    public void putFile() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://s201:8020/");
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream fous = fs.create(new Path("/user/centos/b.txt"), true);

        fous.write("write a new line".getBytes());
        fous.close();

    }


}
