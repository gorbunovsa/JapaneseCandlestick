import java.util.Date;
import java.util.Objects;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

public class candle {

    private static int candleWidth;
    private static String candleSecurities;
    private static String candleDateFrom;
    private static String candleDateTo;
    private static String candleTimeFrom;
    private static String candleTimeTo;
    private static int candleNumReducers;

    static class DateTimeStr {
        private final static SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMddHHmmssSSS");

        private Date date;

        public DateTimeStr(String date) {
            try {
                this.date = fmt.parse(date);
            } catch (ParseException e) {
                System.out.println(e.getMessage());
            }
        }

        public long getLong() {
            return date.getTime();
        }

        public String candleStart(int candle_width) {
            String str_date = fmt.format(date);
            String str_start = str_date.substring(0,8) + "000000000";
            Date start_date = new Date();
            try {
                start_date = fmt.parse(str_start);
            } catch (ParseException e) {
                System.out.println(e.getMessage());
            }
            long diff = date.getTime() - (date.getTime() - start_date.getTime()) % candle_width;
            Date candle_start = new Date();
            candle_start.setTime(diff);
            return fmt.format(candle_start);
        }
    }


    public static class TextPair implements WritableComparable<TextPair> {
        private Text left;
        private Text right;

        public TextPair() {
            left = new Text();
            right = new Text();
        }

        public void set(String s1, String s2) {
            left = new Text(s1);
            right = new Text(s2);
        }

        public Text[] get() {
            Text[] result = new Text[2];
            result[0] = left;
            result[1] = right;
            return result;
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            left.readFields(in);
            right.readFields(in);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            left.write(out);
            right.write(out);
        }

        @Override
        public int compareTo(TextPair o) {
            if (this.left.toString().equals(o.left.toString())) {
                return this.right.toString().compareTo(o.right.toString());
            } else {
                return this.left.toString().compareTo(o.left.toString());
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(left, right);
        }

    }

    public static class CandleMapper
            extends Mapper<Object,Text,TextPair,TextPair>{

        final String DELIMITER = ",";

        public void read_config(Context context) {
            Configuration conf = context.getConfiguration();
            candleWidth = conf.getInt("candle.width", 300000);
            candleSecurities = conf.get("candle.securities", ".*");
            candleDateFrom = conf.get("candle.date.from", "19000101");
            candleDateTo = conf.get("candle.date.to", "20200101");
            candleTimeFrom = conf.get("candle.time.from", "1000");
            candleTimeTo = conf.get("candle.time.to", "1800");
            candleNumReducers = conf.getInt("candle.num.reducers", 1);
        }

        private boolean is_valid_date(String cur, String date_from, String date_to,
                                      String time_from, String time_to) {
            String cur_time = cur.substring(8,12);
            return cur.compareTo(date_from + "000000000") >= 0 & cur.compareTo(date_to + "000000000") < 0
                    & cur_time.compareTo(time_from) >= 0 & cur_time.compareTo(time_to) < 0;
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            read_config(context);
            String[] tokens = value.toString().split(DELIMITER);
            if (!tokens[0].equals("#SYMBOL")) {
                if (is_valid_date(tokens[2], candleDateFrom, candleDateTo, candleTimeFrom, candleTimeTo)
                        & tokens[0].matches(candleSecurities)) {

                    TextPair key_out = new TextPair();
                    DateTimeStr cur_date = new DateTimeStr(tokens[2]);
                    key_out.set(tokens[0], cur_date.candleStart(candleWidth));

                    TextPair value_out = new TextPair();
                    value_out.set(tokens[3], tokens[4]);

                    context.write(key_out, value_out);
                }
            }
        }
    }

    public static class CandlePartitioner extends
            Partitioner <TextPair, TextPair> {

        @Override
        public int getPartition(TextPair key, TextPair value, int numPartitions) {
            Text[] k = key.get();
            String candle_start = k[1].toString();
            DateTimeStr candle = new DateTimeStr(candle_start);
            DateTimeStr date_from = new DateTimeStr(candleDateFrom + "000000000");
            long diff = candle.getLong() - date_from.getLong();
            long candle_id = diff / candleWidth;
            return (int)(candle_id % numPartitions);
        }
    }

    public static class CandleReducer
            extends Reducer<TextPair,TextPair,Text,Text> {

        public void reduce(TextPair key, Iterable<TextPair> values,
                           Context context
        ) throws IOException, InterruptedException {
            double open = 0.0,
                high =  -Double.MAX_VALUE,
                low =  Double.MAX_VALUE,
                close = 0.0;
            long min_id = Long.MAX_VALUE,
                max_id = Long.MIN_VALUE;
            for (TextPair pair : values){
                Text[] val = pair.get();
                long cur_id = Long.parseLong(val[0].toString());
                double cur_price =  Double.parseDouble(val[1].toString());
                if (cur_id < min_id) {
                    open = cur_price;
                    min_id = cur_id;
                }
                if (cur_id > max_id) {
                    close = cur_price;
                    max_id = cur_id;
                }
                if (cur_price < low) {
                    low = cur_price;
                }
                if (cur_price > high) {
                    high = cur_price;
                }
            }
            String open_str = String.format("%.1f", open);
            String high_str = String.format("%.1f", high);
            String low_str = String.format("%.1f", low);
            String close_str = String.format("%.1f", close);
            Text[] key_text = key.get();
            context.write(
                new Text(key_text[0].toString() + "," + key_text[1].toString()),
                new Text(open_str + "," + high_str + "," + low_str + "," + close_str)
            );
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: candle <in> <out>");
            System.exit(2);
        }

        conf.set("mapred.textoutputformat.separator", ",");

        candleNumReducers = conf.getInt("candle.num.reducers", 1);

        Job job = new Job(conf, "candle");

        job.setJarByClass(candle.class);

        job.setMapperClass(CandleMapper.class);
        job.setPartitionerClass(CandlePartitioner.class);
        job.setReducerClass(CandleReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(TextPair.class);

        job.setNumReduceTasks(candleNumReducers);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
