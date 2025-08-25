package com.example.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

/**
 * WordCount应用程序 - 支持S3输出版本 (完整正确版)
 * 
 * 功能特性：
 * 1. 支持控制台输出 (用于调试和验证)
 * 2. 支持S3输出保存 (用于持久化存储)
 * 3. 使用内置数据源避免输入路径问题
 * 4. 兼容EMR环境的S3配置
 * 5. 修正了数据sink执行顺序问题
 * 
 * 使用方法：
 * - 无参数: 仅控制台输出
 * - 带S3路径参数: 输出到S3
 */
public class WordCountWithS3Output {

    public static void main(String[] args) throws Exception {
        // 获取执行环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 创建测试数据 - 使用莎士比亚《哈姆雷特》经典独白
        DataSet<String> text = env.fromElements(
            "To be, or not to be,--that is the question:--",
            "Whether 'tis nobler in the mind to suffer",
            "The slings and arrows of outrageous fortune",
            "Or to take arms against a sea of troubles,",
            "And by opposing end them?--To die,--to sleep,--",
            "No more; and by a sleep to say we end",
            "The heartache, and the thousand natural shocks",
            "That flesh is heir to,--'tis a consummation",
            "Devoutly to be wish'd. To die,--to sleep;--",
            "To sleep! perchance to dream:--ay, there's the rub;",
            "For in that sleep of death what dreams may come",
            "When we have shuffled off this mortal coil,",
            "Must give us pause:--there's the respect",
            "That makes calamity of so long life;",
            "For who would bear the whips and scorns of time,",
            "The oppressor's wrong, the proud man's contumely,",
            "The pangs of despis'd love, the law's delay,",
            "The insolence of office, and the spurns",
            "That patient merit of the unworthy takes,",
            "When he himself might his quietus make",
            "With a bare bodkin? who would fardels bear,",
            "To grunt and sweat under a weary life,",
            "But that the dread of something after death,--",
            "The undiscover'd country, from whose bourn",
            "No traveller returns,--puzzles the will,",
            "And makes us rather bear those ills we have",
            "Than fly to others that we know not of?",
            "Thus conscience does make cowards of us all;",
            "And thus the native hue of resolution",
            "Is sicklied o'er with the pale cast of thought;",
            "And enterprises of great pith and moment,",
            "With this regard, their currents turn awry,",
            "And lose the name of action.--Soft you now!",
            "The fair Ophelia!--Nymph, in thy orisons",
            "Be all my sins remember'd."
        );

        // 执行WordCount逻辑
        DataSet<Tuple2<String, Integer>> counts = text
            .flatMap(new Tokenizer())
            .groupBy(0)
            .sum(1);

        // 检查是否需要输出到S3
        if (args.length > 0 && args[0] != null && !args[0].trim().isEmpty()) {
            String outputPath = args[0].trim();
            
            // 确保路径以斜杠结尾
            if (!outputPath.endsWith("/")) {
                outputPath += "/";
            }
            
            System.out.println("=== Saving Results to S3: " + outputPath + " ===");
            
            // 输出到S3 - 使用OVERWRITE模式确保可以重复运行
            counts.writeAsText(outputPath + "wordcount-results", FileSystem.WriteMode.OVERWRITE)
                 .setParallelism(1); // 设置并行度为1，生成单个输出文件
            
            System.out.println("=== S3 Output Configuration Complete ===");
            
            // 执行程序 (S3输出模式)
            env.execute("WordCount with S3 Output - Enhanced Version");
            
        } else {
            System.out.println("=== Console Output Only Mode ===");
            
            // 仅控制台输出模式 - 使用print()
            counts.print();
            
            System.out.println("=== Console Output Complete ===");
        }
        
        System.out.println("=== WordCount Job Completed Successfully ===");
    }

    /**
     * 分词器实现类
     */
    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // 转换为小写并按非字母字符分割
            String[] words = value.toLowerCase().split("\\W+");
            
            // 发出每个有效单词
            for (String word : words) {
                if (word.length() > 0) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        }
    }
}
