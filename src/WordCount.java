/**
 * Created by 78582 on 2017-06-28.
 */

import java.io.IOException;
import java.util.StringTokenizer;


public class WordCount extends Configured implements Tool {

    /*
        import org.apache.hadoop.mapreduce.Mapper
        Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> : 키-값을 입력받고 출력하는 형태로
        map, setup, cleanup 세 개의 메소드로 이루어져 있다.
     */

    pulbic static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable>
    {

        /*
            import org.apache.hadoop.io.IntWritable
            import org.apache.hadoop.io.LongWritable
            import org.apache.hadoop.io.Text

            Writable : Java 데이터 형과 대응되는 자료형으로, Hadoop Mapreduce는 분산처리를 위해
            노드간 통신으로 데이터 교환을 하기 위해서 Serialize를 할 필요가 있다. java의 Serialize는
            범용적으로 필요없는 기능이 포함되어 있기 때문에 무거워 질 수 있어서 Hadoop 자체적으로 Writable이라는 I/F를 제공한다.

             Text : Java에서도 Text가 존재하지만 Hadoop 에서 조금 더 성능 최적화를 위한 Text이다.
                    이것도 Writable과 마찬가지로 Serialize의 기능을 가지고 있을 것이라고 생각된다.

         */

        // static 으로 되어 있는 것은 다수의 mapper 에서 반복적으로 Instance 생성을 하지 않도록 한 것이다.
        private fianl static IntWritable one = new IntWritable(1);
        private Text word = new Text();


        /*
            void map(KEYIN, VALUEIN, Context) : 입력 레코드 1개에 1회씩 반복적으로 호출되는데,
            Reducer에 데이터를 전달하기 위해 Context 인스턴스의 write 를 통해 출력한다. Context를 통해
            MapReduce 잡설정이나 입출력 데이터에 접근할 수 있다.

            void setup(Context) : Map 단계 시작 직전에 필요한 처리를 수행
            void cleanup(Context) : Map 단계 종료 직전에 필요한 처리를 수행

            아래에서 들어오는 map 함수의 key 값은 LongWritable 타입으로 읽어야할 텍스트의 offset이 들어 있다.
            Text 타입의 value 값에서는 InputSplit 이 있다.
         */

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            //StringTokenizer 는 Java에서 제공하는 클래스로, new StringTokenizer('Text', 'token') 과 같이 사용하며, token 생략시 공백으로 나눔.
            StringTokenizer itr = new StringTokenizer (value.toString());
            while (itr.hasMoreToken()){
                word.set(itr.nextTokens());

                // context에 key-value 값을 write 함.
                context.write(word,one);
            }
        }
    }

    /*
        Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> : Mapper 클래스와 유사하다.

     */


    pulbic static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        private IntWritable result = new IntWritable();

        /*
            void recduce<KEYIN, Iterable<VALUEIN>, Context> : map 메소드와 비슷하지만, value 값이 Iterable 하다.
            reduce 메소드는 하나의 key 에 대해서 한 번만 호출된다. 그렇기 때문에 메소드 내부에서 같은 키를 가진 value들을
            루프를 통해서 처리하도록 기술해야 한다.

            void setup(Context) : Recude 단계 시작 직전에 필요한 처리를 수행
            void cleanup(Context) : Reduce 단계 종료 직전에 필요한 처리를 수행

         */

        public void reduce(Text key, Intrable<IntWritable> values, Context context) throws IOException, InterruptedException
        {

            int sum = 0;
            for (IntWritable val : values )
            {
                sum += val.get();

            }

            result.set(sum);
            context.write(key, result);

        }
    }


    // main 의 소스부분을 따로 떼어 놓은 것.

    pulbic int run(String[] args) throws Exception
    {

        // 인자값이 제대로 들어왔는지 확인
        if(args.length <= 2)
        {
            System.err.printIn("Usage : wordcount <in> <out>");
            System.exit(2);
        }

        /*
          import org.apache.hadoop.mapreduce.Job;

          Configuration 인스턴스와 잡이름"word count"로 Job을 생성함.
          getConf 는 Job의 method???

        */

        Job job = Job.getInstance(getConf(), "word count");

        // Map 은 InputSplit 에 따라서 태스크 갯수가 설정되지만,
        // Reduce는 Map결과 출력되는 key의 수를 고려하여 에 따라 사용자가 지정해줘야 한다.
        job.setNumReduceTasks(2);

        // Main Class를 인수로 mapred.jar를 수행시킨다. 잡을 등록하면, mapred.jar 에 지정한 jar파일이 HDFS 상에 업로드 되고,
        // 그것이 슬레이브 노드로 배포가 되어 수행되는 구조로 되어 있다. 일반적으로 main, mapper, reducer 각각 jar로 생성하여 설정한다.
        job.setJarByClass(WordCount.class);

        // 각 단계에서 어떤 class를 사용할지 정의하는 것이며, 결과적으로 Configuration 인스턴스에 클래스와 관련된 속성값이 저장된다.
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        // map 의 output key, value 타입 설정
        job.setMapOutputKeyClasS(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // reduce 의 output key, value 타입 설정
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        /*

            import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
            import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

            InputFormat/OutputFormat : 데이터 입출력을 위한 모듈. 입출력 대상에 따라서 TextInputFormat/TextOutputFormat,
            DBOInputFormat/DBOOutputFormat, SequenceFileInputformat, SequenceFileOutputFormat 이 있다.

         */

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // 입출력을 위한 디렉토리 지정
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        // CDH4에서 Hadoop 1.x 버전의 MapReduce를 사용할 때 발생하는 문제로 생략하면, 에러메세지 발생하지만 동작에는 문제없다.
        job.getConfiguration().setBoolean("mapred.used.genericoptionsparser", true);


        /*
            boolean waitForCompletion(boolean verbos) : 위에서 설정한 작업을 submit 메소드로 등록하고
            등록이 끝날 때까지 기다리는 작업이다. waitForComplete 수행 없이 submit을 직접 수행하게되면, 상태 완료를 확인 안하고
             바로 리턴된다. 이 방법을 이용해서 복수의 잡을 한꺼번에 투입하는 코드를 만들 수 있다. 그리고.. 잡 등록중 강제종료(ctrl+c)
             를 수행해도 등록된 작업은 취소되지 않는다. 잡을 투입하면서 프로그램 내에서 조건으로 잡을 중지하고 싶다면 killjob 메소드를 사용하면 된다.

         */
        return (job.waitForCompletion(true) ? 0: 1);
    }

    public static void main (String[] args) throws Exception
    {
        /*
            import org.apache.hadoop.config.Configuration;
            import org.apache.hadoop.config.Configured;
            import org.apache.hadoop.tuil.ToolRunner;

            ToolRunner : hadoop에서 사용되는 GenericOptionsParser를 이용하여 공통 옵션 인수를 해석하고
            Configuration 인스턴스에 결과를 저장한 후 Tool.run 을 호출한다.

            ToolRunner.run(Configuration, Tool, String[]) : Configuration 객체가 null이라면 그것을 생성하고,
            Tool 객체와 인자값으로 Configuration 객채를 초기화 한다.

         */
        int res = ToolRunner.run(new Configuration(), new WordCount(), args);
        System.exit(res);
    }
}
