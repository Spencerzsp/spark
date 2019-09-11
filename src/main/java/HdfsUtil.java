import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;

public class HdfsUtil {
    private static String defaultFS = "hdfs://39.108.144.231:8020";

    public static void main(String[] args) throws Exception {
        //创建configuration对象
        Configuration conf = new Configuration();
        //配置在etc/hadoop/core-site.xml   fs.defaultFS
        conf.set("fs.defaultFS", defaultFS);
//        conf.set("hadoop.home.dir", "D:/1gouwei2019s/hadoop-2.6.5");
        //创建FileSystem对象
//        查看hdfs集群服务器/user/passwd.txt的内容
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream is = fs.open(new Path(defaultFS + "/user/root/word.txt"));
        File file = new File("a.txt");
        OutputStream os = new FileOutputStream(file);
        byte[] buff = new byte[1024];
        int length = 0;
        while ((length = is.read(buff)) != -1) {
            System.out.println(new String(buff, 0, length));
            os.write(buff, 0, length);
            os.flush();
        }

    }
}
