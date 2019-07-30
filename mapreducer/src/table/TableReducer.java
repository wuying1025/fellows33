package table;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class TableReducer extends Reducer<Text,TableBean,TableBean,NullWritable>{
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        TableBean t = new TableBean();//01 小米
        List<TableBean> lt = new ArrayList<TableBean>();
        for (TableBean tb:values) {
            if (tb.getType().equals("0")) {//当前为order
                try {
                    TableBean t1 = new TableBean();
                    BeanUtils.copyProperties(t1, tb);
                    lt.add(t1);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {//当前为product
                try {
                    BeanUtils.copyProperties(t, tb);
                } catch (Exception e) {
                }
            }
        }
        for (TableBean tableBean : lt) {
            // 1001 01 1  小米
            tableBean.setPname(t.getPname());
            context.write(tableBean,NullWritable.get());
        }
    }
}
