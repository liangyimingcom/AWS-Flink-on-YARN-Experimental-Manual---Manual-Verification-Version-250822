package com.example.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

/**
 * WordCount应用程序 - S3输入版本
 * 
 * 功能特性：
 * 1. 从S3读取输入文件
 * 2. 支持控制台输出和S3输出双模式
 * 3. 兼容EMR环境的S3配置
 * 4. 完整的错误处理和日志记录
 * 
 * 使用方法：
 * - 单参数: flink run app.jar s3://bucket/input/
 * - 双参数: flink run app.jar s3://bucket/input/ s3://bucket/output/
 * 
 * 参数说明：
 * args[0] - 输入路径 (必需): S3输入文件路径
 * args[1] - 输出路径 (可选): S3输出路径，如果不提供则输出到控制台
 * 
 * 示例：
 * flink run -m yarn-cluster app.jar s3://flink-emr-lab-1756039468/input/
 * flink run -m yarn-cluster app.jar s3://flink-emr-lab-1756039468/input/ s3://flink-emr-lab-1756039468/output/
 */
public class WordCountS3Input {

    public static void main(String[] args) throws Exception {
        // 获取执行环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 参数验证
        if (args.length < 1) {
            System.err.println("错误: 缺少输入路径参数");
            System.err.println("使用方法:");
            System.err.println("  flink run app.jar <input-path> [output-path]");
            System.err.println("示例:");
            System.err.println("  flink run app.jar s3://bucket/input/");
            System.err.println("  flink run app.jar s3://bucket/input/ s3://bucket/output/");
            System.exit(1);
        }

        String inputPath = args[0].trim();
        String outputPath = args.length > 1 ? args[1].trim() : null;

        // 验证输入路径
        if (inputPath.isEmpty()) {
            System.err.println("错误: 输入路径不能为空");
            System.exit(1);
        }

        System.out.println("=== WordCount S3输入版本启动 ===");
        System.out.println("输入路径: " + inputPath);
        System.out.println("输出路径: " + (outputPath != null ? outputPath : "控制台"));

        try {
            // 从S3读取输入数据
            System.out.println("=== 正在从S3读取输入数据 ===");
            DataSet<String> text = env.readTextFile(inputPath);

            // 执行WordCount逻辑
            System.out.println("=== 开始执行WordCount处理 ===");
            DataSet<Tuple2<String, Integer>> counts = text
                .flatMap(new Tokenizer())
                .groupBy(0)
                .sum(1);

            // 根据参数决定输出方式
            if (outputPath != null && !outputPath.trim().isEmpty()) {
                // S3输出模式
                String finalOutputPath = outputPath.trim();
                if (!finalOutputPath.endsWith("/")) {
                    finalOutputPath += "/";
                }
                
                System.out.println("=== S3输出模式: " + finalOutputPath + " ===");
                
                // 输出到S3
                counts.writeAsText(finalOutputPath + "wordcount-results", FileSystem.WriteMode.OVERWRITE)
                     .setParallelism(1); // 生成单个输出文件
                
                System.out.println("=== S3输出配置完成 ===");
                
                // 执行程序
                env.execute("WordCount S3 Input to S3 Output");
                
            } else {
                // 控制台输出模式
                System.out.println("=== 控制台输出模式 ===");
                
                // 输出到控制台
                counts.print();
                
                System.out.println("=== 控制台输出完成 ===");
            }
            
            System.out.println("=== WordCount作业成功完成 ===");
            
        } catch (Exception e) {
            System.err.println("=== WordCount作业执行失败 ===");
            System.err.println("错误信息: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

    /**
     * 分词器实现类
     * 将输入的文本行分解为单词，并转换为(word, 1)的元组
     */
    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // 跳过空行
            if (value == null || value.trim().isEmpty()) {
                return;
            }
            
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
