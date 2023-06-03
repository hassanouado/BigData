import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
public class VenteMapper extends Mapper<LongWritable, Text,Text, FloatWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
       String vents[]= value.toString().split(" ");
            String annee = vents[0].split("/")[2];
            //context.write(new Text(vents[1]),new FloatWritable(Float.parseFloat(vents[3])));
           context.write(new Text(vents[1]+annee),new FloatWritable(Float.parseFloat(vents[3])));
    }
}
