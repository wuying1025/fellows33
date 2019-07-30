package tableCache;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class TableCacheMapper extends Mapper<LongWritable,Text,Text,NullWritable>{
    Map<String, String> pdMap = new HashMap<>();// <01,小米>   02 华为   03 格力
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
            throws IOException, InterruptedException {
        // 1 获取缓存的文件
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("d:/cache/pd.txt")));
        String line;
        while(StringUtils.isNotEmpty(line = reader.readLine())){
            // 2 切割
            String[] fields = line.split("\t");
            // 3 缓存数据到集合
            pdMap.put(fields[0], fields[1]);
        }
        // 4 关流
        reader.close();
    }


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\t");//1001  01  1
        String k = words[0]+"\t"+pdMap.get(words[1])+"\t"+words[2];
        context.write(new Text(k),NullWritable.get());
    }
}
