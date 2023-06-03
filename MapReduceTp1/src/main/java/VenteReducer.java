import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;
public class VenteReducer extends Reducer<Text, FloatWritable,Text, DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Reducer<Text, FloatWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
            Iterator<FloatWritable> it=values.iterator();
            int somme_vent=0;
            while (it.hasNext()){
                somme_vent+=it.next().get();
            }
            context.write(key,new DoubleWritable(somme_vent));
     }
}
