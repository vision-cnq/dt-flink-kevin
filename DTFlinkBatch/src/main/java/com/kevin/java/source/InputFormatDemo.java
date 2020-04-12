package com.kevin.java.source;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * @author caonanqing
 * @version 1.0
 * @description         数据源创建数据集
 *      例如：从文件或java集合，创建数据集的一般机制在InputFormat之后抽象，。
 *      Flink只带几种内置格式。
 *      基于文件：
 *          readTextFile：逐行读取文件，并将其作为字符串返回。
 *          readTextFileWithValue(path)/ TextValueInputFormat-逐行读取文件，并将它们作为StringValues返回。StringValue是可变的字符串。
 *          readCsvFile(path)/ CsvInputFormat-解析以逗号（或其他字符）分隔的字段的文件。返回元组或POJO的数据集。支持基本的Java类型及其与Value相对应的字段类型。
 *          readFileOfPrimitives(path, Class)/ PrimitiveInputFormat-解析以换行符（或其他char序列）分隔的原始数据类型的文件，例如String或Integer。
 *          readFileOfPrimitives(path, delimiter, Class)// PrimitiveInputFormat-解析以换行符（或其他char序列）分隔的原始数据类型的文件，例如String或Integer使用给定的分隔符。
 *      基于集合：
 *          fromCollection(Collection)-从Java.util.Collection创建数据集。集合中的所有元素必须具有相同的类型。
 *          fromCollection(Iterator, Class)-从迭代器创建数据集。该类指定迭代器返回的元素的数据类型。
 *          fromElements(T ...)-从给定的对象序列创建数据集。所有对象必须具有相同的类型。
 *          fromParallelCollection(SplittableIterator, Class)-从迭代器并行创建数据集。该类指定迭代器返回的元素的数据类型。
 *          generateSequence(from, to) -并行生成给定间隔中的数字序列
 *      通用：
 *          readFile(inputFormat, path)/ FileInputFormat-接受文件输入格式。
 *          createInput(inputFormat)/ InputFormat-接受通用输入格式。
 *
 *       详细请查阅官方：https://ci.apache.org/projects/flink/flink-docs-release-1.10/zh/dev/batch/#collapse-198
 *
 * @createDate 2020/3/12
 */
public class InputFormatDemo {

    public static void main(String[] args) {

        // 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 读取本地文件
        DataSource<String> localLines = env.readTextFile("DTFlinkBatch\\src\\main\\resources\\test.txt");
        // 读取hdfs文件
        DataSource<String> hdfsLines = env.readTextFile("hdfs://Master:/9000/to/my/textfile");
        // 读取hdfs中csv文件的三个字段
        DataSource<Tuple3<Integer, String, Double>> csvInput = env.readCsvFile("hdfs://Master:/9000/to/my/textfile")
                .types(Integer.class, String.class, Double.class);
        // 将包含三个字段的CSV文件读入包含相应字段的POJO (Person.class)
        // DataSet<Person>> csvInput = env.readCsvFile("hdfs://Master:/9000/to/my/textfile").pojoType(Person.class, "name", "age", "zipcode");
        // 从类型为SequenceFileInputFormat的指定路径读取文件
        // DataSet<Tuple2<IntWritable, Text>> tuples = env.createInput(HadoopInputs.readSequenceFile(IntWritable.class, Text.class, "hdfs://Master:/9000/to/my/textfile"));

        // 从一些给定的元素创建一个集合
        DataSet<String> value = env.fromElements("Foo", "bar", "foobar", "fubar");
        // 生成一个数字序列
        DataSet<Long> numbers = env.generateSequence(1, 10000000);
        // 使用JDBC输入格式从关系数据库读取数据
        /*DataSet<Tuple2<String, Integer> dbData =
                env.createInput(
                        JDBCInputFormat.buildJDBCInputFormat()
                                .setDrivername("org.apache.derby.jdbc.EmbeddedDriver")
                                .setDBUrl("jdbc:derby:memory:persons")
                                .setQuery("select name, age from persons")
                                .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO))
                                .finish()
                );*/
    }
}
