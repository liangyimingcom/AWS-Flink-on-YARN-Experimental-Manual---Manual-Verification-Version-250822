# AWS Flink on YARN 实验手册 - 手动验证版本

## 文档版本信息
- **版本**: 4.0 (完整正确版)
- **状态**: ✅ 在AWS环境中，以下的代码和配置方式经过了100%验证通过，支持控制台和S3双输出
- **两个Flink实验带Jar包: ，请看附件 《Flink应用程序V1》、《Flink应用程序V2》**
- **学习步骤**: 
  - 建议学员按照V1→V2的顺序进行学习，先掌握基础概念，再深入生产级应用开发。

### 🎯 本版本特色
1. **双输出模式**: 支持控制台输出和S3持久化存储
2. **完全验证**: 所有功能都经过实际EMR集群测试
4. **生产就绪**: 可直接用于生产环境部署

### 📊 验证数据
- **控制台输出**: 24秒完成，169个单词处理
- **S3输出**: 28秒完成，1.7KB结果文件
- **处理性能**: 13.343秒Flink执行时间
- **输出验证**: S3文件包含完整的词频统计结果



## 概述

本实验手册提供了在Amazon EMR上部署和运行Apache Flink on YARN的完整指南，支持控制台输出和S3持久化存储两种模式。所有内容都基于实际部署验证，确保100%可用。

## 前提条件

### 必需的AWS服务权限
- Amazon EMR (创建和管理集群)
- Amazon S3 (存储数据和日志)
- Amazon EC2 (密钥对管理)
- AWS IAM (角色管理)

### 本地环境要求
- AWS CLI 已配置 (推荐版本 2.x)
- Java 8 或 11 (用于本地开发)
- Maven 3.6+ (用于构建应用程序)



## 第一部分：环境准备

### 1.0.1 AWS CLI 安装和配置指南

#### 对于 Windows 用户

1. **下载安装程序**:
   - 访问 [AWS CLI 官方下载页面](https://aws.amazon.com/cli/)
   - 下载最新版本的 Windows 安装程序 (MSI)

2. **运行安装程序**:
   - 运行下载的 MSI 文件
   - 按照安装向导的提示完成安装

3. **验证安装**:
   ```cmd
   aws --version
   ```

#### 对于 macOS 用户

1. **使用 Homebrew 安装**:
   ```bash
   brew install awscli
   ```

2. **或使用 Python pip 安装**:
   ```bash
   pip3 install awscli --upgrade --user
   ```

3. **验证安装**:
   ```bash
   aws --version
   ```

#### 对于 Linux 用户

1. **使用包管理器安装**:
   
   对于 Ubuntu/Debian:
   ```bash
   sudo apt-get update
   sudo apt-get install awscli
   ```
   
   对于 Amazon Linux/RHEL/CentOS:
   ```bash
   sudo yum install awscli
   ```

2. **或使用 Python pip 安装**:
   ```bash
   pip3 install awscli --upgrade --user
   ```

3. **验证安装**:
   ```bash
   aws --version
   ```



### 1.0.2 创建 AWS Profile 的示例

要创建 AWS Profile，您可以通过以下几种方式配置：

#### 1. 使用 AWS CLI 命令行配置

```bash
aws configure --profile oversea1
```

**执行后会提示输入以下信息**:
- AWS Access Key ID: 输入您的访问密钥ID
- AWS Secret Access Key: 输入您的私有访问密钥
- Default region name: 输入默认区域 (如eu-central-1) <u>[eu-central-1是欧洲法兰克福]</u>
- Default output format: 输入输出格式 (如 json)

#### 2. 直接编辑 AWS 凭证文件

编辑 `~/.aws/credentials` 文件 (Windows 上是 `%USERPROFILE%\.aws\credentials`):

```bash
# 使用文本编辑器打开文件
nano ~/.aws/credentials
```

添加以下内容:

```ini
[oversea1]
aws_access_key_id = YOUR_ACCESS_KEY
aws_secret_access_key = YOUR_SECRET_KEY
```

#### 3. 编辑 AWS 配置文件

编辑 `~/.aws/config` 文件 (Windows 上是 `%USERPROFILE%\.aws\config`):

```bash
# 使用文本编辑器打开文件
nano ~/.aws/config
```

添加以下内容:

```ini
[profile oversea1]
region = eu-central-1
output = json
```

#### 验证配置是否成功

配置完成后，您可以验证身份:

```bash
aws sts get-caller-identity --profile oversea1
```

**预期输出**:

```json
{
    "UserId": "AIDACKCEVSQ6C2EXAMPLE",
    "Account": "153705321444",
    "Arn": "arn:aws:iam::153705321444:user/your-username"
}
```


### 1.1 验证AWS身份

```bash
aws sts get-caller-identity --profile oversea1
```

**预期输出**:
```json
{
    "UserId": "AIDACKCEVSQ6C2EXAMPLE",
    "Account": "153705321444",
    "Arn": "arn:aws:iam::153705321444:user/your-username"
}
```

### 1.2 创建IAM角色

#### 创建EMR服务角色

```bash
# 创建EMR默认角色
aws iam create-role \
    --role-name EMR_DefaultRole \
    --assume-role-policy-document '{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "elasticmapreduce.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }' \
    --profile oversea1

# 附加策略
aws iam attach-role-policy \
    --role-name EMR_DefaultRole \
    --policy-arn arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole \
    --profile oversea1
```

#### 创建EMR EC2实例角色

```bash
# 创建EC2实例角色
aws iam create-role \
    --role-name EMR_EC2_DefaultRole \
    --assume-role-policy-document '{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "ec2.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }' \
    --profile oversea1

# 附加策略
aws iam attach-role-policy \
    --role-name EMR_EC2_DefaultRole \
    --policy-arn arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role \
    --profile oversea1

# ✅ 关键步骤：创建实例配置文件
aws iam create-instance-profile \
    --instance-profile-name EMR_EC2_DefaultRole \
    --profile oversea1

# 将角色添加到实例配置文件
aws iam add-role-to-instance-profile \
    --instance-profile-name EMR_EC2_DefaultRole \
    --role-name EMR_EC2_DefaultRole \
    --profile oversea1
```

### 1.3 创建S3存储桶

```bash
# 生成唯一的存储桶名称
BUCKET_NAME="flink-emr-lab-$(date +%s)"
echo "存储桶名称: $BUCKET_NAME"

# 创建S3存储桶
aws s3 mb s3://$BUCKET_NAME --region eu-central-1 --profile oversea1

# 创建目录结构
aws s3api put-object --bucket $BUCKET_NAME --key input/ --profile oversea1
aws s3api put-object --bucket $BUCKET_NAME --key output/ --profile oversea1
aws s3api put-object --bucket $BUCKET_NAME --key checkpoints/ --profile oversea1
aws s3api put-object --bucket $BUCKET_NAME --key jars/ --profile oversea1
aws s3api put-object --bucket $BUCKET_NAME --key logs/ --profile oversea1

# 保存存储桶名称供后续使用
echo $BUCKET_NAME > bucket_name.txt
```

![image-20250825090641606](./assets/image-20250825090641606.png)



### 1.4 创建EC2密钥对

```bash
# 创建密钥对
aws ec2 create-key-pair \
    --key-name flink-emr-keypair \
    --query 'KeyMaterial' \
    --output text \
    --region eu-central-1 \
    --profile oversea1 > flink-emr-keypair.pem

# 设置密钥权限
chmod 400 flink-emr-keypair.pem
```



## 第二部分：EMR集群创建

### 2.1 创建EMR集群

**✅ 已验证的完整配置**:

```bash
# 创建EMR集群 - 完整正确版
aws emr create-cluster \
    --name "Flink-on-YARN-Cluster_flink" \
    --release-label emr-6.15.0 \
    --applications Name=Hadoop Name=Flink Name=Zeppelin \
    --instance-groups '[
        {
            "Name": "Master",
            "InstanceGroupType": "MASTER",
            "InstanceType": "r6g.xlarge",
            "InstanceCount": 1
        },
        {
            "Name": "Core",
            "InstanceGroupType": "CORE",
            "InstanceType": "r6g.xlarge",
            "InstanceCount": 2
        }
    ]' \
    --configurations '[
        {
            "Classification": "flink-conf",
            "Properties": {
                "taskmanager.memory.process.size": "20480m",
                "jobmanager.memory.process.size": "8192m",
                "taskmanager.numberOfTaskSlots": "4",
                "classloader.check-leaked-classloader": "false"
            }
        },
        {
            "Classification": "yarn-site",
            "Properties": {
                "yarn.nodemanager.resource.memory-mb": "24576",
                "yarn.nodemanager.vmem-check-enabled": "false"
            }
        }
    ]' \
    --ec2-attributes KeyName=flink-emr-keypair,InstanceProfile=EMR_EC2_DefaultRole \
    --service-role EMR_DefaultRole \
    --region eu-central-1 \
    --log-uri s3://$BUCKET_NAME/logs/ \
    --profile oversea1

```

~~~
配置参数已优化:
实例类型从 m5.xlarge 更改为 r6g.xlarge (4 vCore, 30.5 GiB 内存)
TaskManager 内存从 2048m 增加到 20480m (20GB)，充分利用更大的内存
JobManager 内存从 1024m 增加到 8192m (8GB)
TaskManager 槽位数从 2 增加到 4，匹配 r6g.xlarge 的 4 个 vCore
YARN NodeManager 资源内存从 3072MB 增加到 24576MB (24GB)，为应用程序保留一些系统内存
~~~



![image-20250825092329396](./assets/image-20250825092329396.png)



#### EMR集群配置 - AWS Graviton 版本

以下是将所有实例类型更新为 AWS Graviton 处理器的 EMR 集群配置参考表和配置示例：

#### EC2 Graviton 实例类型与内存配置参考

| 实例类型    | vCPU | 内存(GiB) | 推荐YARN内存配置(GB) | 推荐TaskManager内存(GB) |
| ----------- | ---- | --------- | -------------------- | ----------------------- |
| m6g.large   | 2    | 8         | 5-6                  | 4                       |
| m6g.xlarge  | 4    | 16        | 12-13                | 8-10                    |
| m6g.2xlarge | 8    | 32        | 26-28                | 20-24                   |
| m6g.4xlarge | 16   | 64        | 56-58                | 48-52                   |
| r6g.xlarge  | 4    | 32        | 28-29                | 24-26                   |
| r6g.2xlarge | 8    | 64        | 58-60                | 52-56                   |
| c6g.2xlarge | 8    | 16        | 12-13                | 8-10                    |
||||||

建议根据实际工作负载测试和调整这些参数，确保最佳性能和资源利用率。

#### 配置示例

#### 内存密集型工作负载 (r6g.xlarge)
```json
{
  "Classification": "yarn-site",
  "Properties": {
    "yarn.nodemanager.resource.memory-mb": "28672", // r6g.xlarge上为28GB
    "yarn.scheduler.maximum-allocation-mb": "28672",
    "yarn.nodemanager.vmem-check-enabled": "false"
  }
},
{
  "Classification": "flink-conf",
  "Properties": {
    "taskmanager.memory.process.size": "24576m", // 24GB
    "jobmanager.memory.process.size": "8192m",
    "taskmanager.numberOfTaskSlots": "4"
  }
}
```

#### 计算密集型工作负载 (c6g.2xlarge)
```json
{
  "Classification": "yarn-site",
  "Properties": {
    "yarn.nodemanager.resource.memory-mb": "12288", // c6g.2xlarge上为12GB
    "yarn.nodemanager.resource.cpu-vcores": "8",
    "yarn.scheduler.maximum-allocation-vcores": "8"
  }
},
{
  "Classification": "flink-conf",
  "Properties": {
    "taskmanager.memory.process.size": "9216m", // 9GB
    "jobmanager.memory.process.size": "2048m",
    "taskmanager.numberOfTaskSlots": "8",
    "parallelism.default": "16"
  }
}
```

#### 大规模流处理 (m6g.2xlarge)
```json
{
  "Classification": "yarn-site",
  "Properties": {
    "yarn.nodemanager.resource.memory-mb": "26624", // m6g.2xlarge上为26GB
    "yarn.nodemanager.vmem-check-enabled": "false"
  }
},
{
  "Classification": "flink-conf",
  "Properties": {
    "taskmanager.memory.process.size": "20480m", // 20GB
    "jobmanager.memory.process.size": "4096m",
    "taskmanager.numberOfTaskSlots": "8",
    "state.backend": "rocksdb",
    "state.backend.incremental": "true",
    "taskmanager.memory.managed.fraction": "0.4"
  }
}
```



<u>下面步骤可以略过：：：</u>

**注意：AWS Graviton 处理器基于 ARM 架构**，在迁移时请确保您的应用程序代码和依赖项与 ARM 架构兼容。大多数 Java 应用程序应该可以直接运行，但如果有本地代码或特定依赖项，可能需要重新编译。

```
# 创建EMR集群 - 基于x86的CPU架构 m5.xlarge

aws emr create-cluster \
    --name "Flink-on-YARN-Cluster_flink" \
    --release-label emr-6.15.0 \
    --applications Name=Hadoop Name=Flink Name=Zeppelin \
    --instance-groups '[
        {
            "Name": "Master",
            "InstanceGroupType": "MASTER",
            "InstanceType": "m5.xlarge",
            "InstanceCount": 1
        },
        {
            "Name": "Core",
            "InstanceGroupType": "CORE",
            "InstanceType": "m5.xlarge",
            "InstanceCount": 2
        }
    ]' \
    --configurations '[
        {
            "Classification": "flink-conf",
            "Properties": {
                "taskmanager.memory.process.size": "2048m",
                "jobmanager.memory.process.size": "1024m",
                "taskmanager.numberOfTaskSlots": "2",
                "classloader.check-leaked-classloader": "false"
            }
        },
        {
            "Classification": "yarn-site",
            "Properties": {
                "yarn.nodemanager.resource.memory-mb": "3072",
                "yarn.nodemanager.vmem-check-enabled": "false"
            }
        }
    ]' \
    --ec2-attributes KeyName=flink-emr-keypair,InstanceProfile=EMR_EC2_DefaultRole \
    --service-role EMR_DefaultRole \
    --region eu-central-1 \
    --log-uri s3://$BUCKET_NAME/logs/ \
    --profile oversea1
```

##### x86的CPU架构 EC2实例类型与内存配置参考

| 实例类型   | vCPU | 内存(GiB) | 推荐YARN内存配置(GB) | 推荐TaskManager内存(GB) |
| ---------- | ---- | --------- | -------------------- | ----------------------- |
| m5.large   | 2    | 8         | 5-6                  | 4                       |
| m5.xlarge  | 4    | 16        | 12-13                | 8-10                    |
| m5.2xlarge | 8    | 32        | 26-28                | 20-24                   |
| m5.4xlarge | 16   | 64        | 56-58                | 48-52                   |
| r5.xlarge  | 4    | 32        | 28-29                | 24-26                   |
| r5.2xlarge | 8    | 64        | 58-60                | 52-56                   |
| c5.2xlarge | 8    | 16        | 12-13                | 8-10                    |

技术博客参考：

- [EMR 上的 Spark 作业优化实践 ](https://aws.amazon.com/cn/blogs/china/spark-job-ptimization-practice-on-emr/) 
- [在 Amazon EMR 上成功管理 Apache Spark 应用程序内存的最佳实践](https://aws.amazon.com/cn/blogs/china/best-practices-for-successfully-managing-memory-for-apache-spark-applications-on-amazon-emr-8770-2/)



### 2.2 监控集群状态

```bash
# 获取集群ID (从上一步输出中获取)
CLUSTER_ID="j-PSJC020KDGT6"
echo $CLUSTER_ID > cluster_id.txt

# 直接检查集群状态
STATUS=$(aws emr describe-cluster --cluster-id $CLUSTER_ID --query 'Cluster.Status.State' --output text --region eu-central-1 --profile oversea1)
echo "集群状态: $STATUS"

# 根据状态输出结果
if [ "$STATUS" = "WAITING" ] || [ "$STATUS" = "RUNNING" ]; then
    echo "集群已就绪或正在运行"
elif [ "$STATUS" = "STARTING" ] || [ "$STATUS" = "BOOTSTRAPPING" ]; then
    echo "集群正在启动中"
elif [ "$STATUS" = "TERMINATED" ] || [ "$STATUS" = "TERMINATED_WITH_ERRORS" ]; then
    echo "集群已终止或终止时出错"
else
    echo "集群状态: $STATUS"
fi

```

**预期时间**: 集群启动大约需要7分钟

![image-20250825092817962](./assets/image-20250825092817962.png)

![image-20250825100643391](./assets/image-20250825100643391.png)

![image-20250825100753018](./assets/image-20250825100753018.png)



## 第三部分：Flink应用程序开发

### 3.1 创建Maven项目

```bash
# 创建项目目录
mkdir flink-app && cd flink-app

# 创建Maven项目结构
mkdir -p src/main/java/com/example/flink
```


### 3.2 创建pom.xml
```bash
# 创建
vim pom.xml
```

**✅ 完整验证的Maven配置**:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.example</groupId>
    <artifactId>flink-emr-s3-examples</artifactId>
    <version>1.0-SNAPSHOT</version>
    <packaging>jar</packaging>

    <properties>
        <maven.compiler.source>8</maven.compiler.source>
        <maven.compiler.target>8</maven.compiler.target>
        <flink.version>1.17.1</flink.version>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    </properties>

    <dependencies>
        <!-- Flink核心依赖 -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-java</artifactId>
            <version>${flink.version}</version>
            <scope>provided</scope>
        </dependency>
        
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-streaming-java</artifactId>
            <version>${flink.version}</version>
            <scope>provided</scope>
        </dependency>
        
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-clients</artifactId>
            <version>${flink.version}</version>
            <scope>provided</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.8.1</version>
                <configuration>
                    <source>8</source>
                    <target>8</target>
                </configuration>
            </plugin>
            
            <!-- ✅ 完整的Shade插件配置 -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <version>3.2.4</version>
                <executions>
                    <execution>
                        <phase>package</phase>
                        <goals>
                            <goal>shade</goal>
                        </goals>
                        <configuration>
                            <transformers>
                                <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                                    <mainClass>com.example.flink.WordCountWithS3Output</mainClass>
                                </transformer>
                            </transformers>
                            <filters>
                                <filter>
                                    <artifact>*:*</artifact>
                                    <excludes>
                                        <exclude>META-INF/*.SF</exclude>
                                        <exclude>META-INF/*.DSA</exclude>
                                        <exclude>META-INF/*.RSA</exclude>
                                    </excludes>
                                </filter>
                            </filters>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```



### 3.3 创建WordCount应用程序

**✅ 支持双输出模式的完整应用**: WordCountWithS3Output.java

```bash
# 创建源码在准确的项目目录（很重要，否则后面emr执行报错）
# 创建Maven项目结构
vim src/main/java/com/example/flink/WordCountWithS3Output.java
```

```java
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
```

```
源码关键特性分析 这个 WordCountWithS3Output 类是一个灵活的 Flink WordCount 应用，具有以下特点：

双模式输出支持：
无参数时：结果输出到控制台（使用 counts.print()）
有参数时：结果输出到指定的 S3 路径（使用 counts.writeAsText()）

内置数据源：
使用 env.fromElements() 创建了一个包含莎士比亚《哈姆雷特》独白的内置数据集
不需要外部输入文件，简化了测试和部署

S3 输出优化：
自动处理路径末尾的斜杠
使用 FileSystem.WriteMode.OVERWRITE 确保可重复运行
设置并行度为 1，生成单个输出文件

标准 WordCount 处理逻辑：
使用 flatMap 进行分词
使用 groupBy 和 sum 进行单词计数

三个作业的执行分析
1. Flink-Built-in-WordCount
flink run -m yarn-cluster /usr/lib/flink/examples/batch/WordCount.jar
这个作业运行的是 Flink 自带的 WordCount 示例，不是我们看到的源码。Flink 内置的 WordCount 示例通常需要输入和输出路径参数，但这里没有提供，可能使用了默认值或示例数据。

2. Flink-WordCount-S3-Output
bash -c "aws s3 cp s3://flink-emr-lab-1756083935/jars/wordcount-s3-app.jar /tmp/app.jar && flink run -m yarn-cluster /tmp/app.jar s3://flink-emr-lab-1756083935/output/"
这个作业运行的是我们看到的 WordCountWithS3Output 源码，并且传递了 S3 路径参数：
程序会进入 S3 输出模式（if (args.length > 0) 分支）
处理结果会写入到 s3://flink-emr-lab-1756083935/output/wordcount-results 文件
输出使用 OVERWRITE 模式，如果文件已存在会被覆盖
由于设置了并行度为 1，会生成单个输出文件

3. Flink-WordCount-Console-Output
bash -c "aws s3 cp s3://flink-emr-lab-1756083935/jars/wordcount-s3-app.jar /tmp/app.jar && flink run -m yarn-cluster /tmp/app.jar"
这个作业也运行 WordCountWithS3Output 源码，但没有传递参数：

程序会进入控制台输出模式（else 分支）
使用 counts.print() 将结果输出到控制台
不会生成任何 S3 输出文件

关键区别总结
第一个作业 使用 Flink 内置的 WordCount 示例，与我们分析的源码无关。

第二个作业 和 第三个作业 使用相同的自定义 JAR（我们分析的源码），但：
第二个作业提供了 S3 路径参数，结果保存到 S3
第三个作业没有提供参数，结果仅输出到控制台

数据源：所有三个作业都使用内置数据，不需要外部输入文件
自定义代码使用《哈姆雷特》独白文本
Flink 内置示例可能使用其他示例文本

```

### 3.4 构建应用程序

```bash
# 编译和打包
mvn clean package

# 验证JAR文件
ls -la target/flink-emr-s3-examples-1.0-SNAPSHOT.jar

# 验证JAR文件 检查类是否确实包含在 JAR 中
jar tvf target/flink-emr-s3-examples-1.0-SNAPSHOT.jar | grep WordCountWithS3Output
#  5165 Mon Aug 25 09:56:22 CST 2025 com/example/flink/WordCountWithS3Output.class
#  2032 Mon Aug 25 09:56:22 CST 2025 com/example/flink/WordCountWithS3Output$Tokenizer.class
```

![image-20250825093155463](./assets/image-20250825093155463.png)

![image-20250825095657060](./assets/image-20250825095657060.png)



### 3.5 上传JAR到S3

```bash
# 上传应用程序JAR
aws s3 cp target/flink-emr-s3-examples-1.0-SNAPSHOT.jar \
    s3://$BUCKET_NAME/jars/wordcount-s3-app.jar \
    --profile oversea1
```

![image-20250825093227052](./assets/image-20250825093227052.png)

![image-20250825093253979](./assets/image-20250825093253979.png)



## 第四部分：运行Flink作业

### 4.1 方式一：控制台输出模式 (✅ 推荐用于测试)

```bash
# 运行控制台输出版本
aws emr add-steps \
    --cluster-id $CLUSTER_ID \
    --steps '[
        {
            "Name": "Flink-WordCount-Console-Output",
            "ActionOnFailure": "CONTINUE",
            "Jar": "command-runner.jar",
            "Args": [
                "bash", "-c",
                "aws s3 cp s3://'$BUCKET_NAME'/jars/wordcount-s3-app.jar /tmp/app.jar && flink run -m yarn-cluster /tmp/app.jar"
            ]
        }
    ]' \
    --region eu-central-1 \
    --profile oversea1
```

**预期结果**: 

- 执行时间: ~24秒
- 处理结果: 169个单词的词频统计
- 输出位置: 作业日志中的stdout

![image-20250825100447960](./assets/image-20250825100447960.png)

![image-20250825100217551](./assets/image-20250825100217551.png)

![image-20250825100345487](./assets/image-20250825100345487.png)



### 4.2 方式二：S3输出模式 (✅ 推荐用于生产)

```bash
# 运行S3输出版本
aws emr add-steps \
    --cluster-id $CLUSTER_ID \
    --steps '[
        {
            "Name": "Flink-WordCount-S3-Output",
            "ActionOnFailure": "CONTINUE",
            "Jar": "command-runner.jar",
            "Args": [
                "bash", "-c",
                "aws s3 cp s3://'$BUCKET_NAME'/jars/wordcount-s3-app.jar /tmp/app.jar && flink run -m yarn-cluster /tmp/app.jar s3://'$BUCKET_NAME'/output/"
            ]
        }
    ]' \
    --region eu-central-1 \
    --profile oversea1
```

**预期结果**:
- 执行时间: ~28秒
- 处理结果: 169个单词的词频统计
- 输出位置: `s3://bucket/output/wordcount-results`
- 文件大小: ~1.7KB

![image-20250825100528662](./assets/image-20250825100528662.png)

![image-20250825101402806](./assets/image-20250825101402806.png)

![image-20250825101457346](./assets/image-20250825101457346.png)



### 4.3 方式三：内置示例验证 (✅ 环境验证)

```bash
# 运行内置WordCount示例验证环境
aws emr add-steps \
    --cluster-id $CLUSTER_ID \
    --steps '[
        {
            "Name": "Flink-Built-in-WordCount",
            "ActionOnFailure": "CONTINUE",
            "Jar": "command-runner.jar",
            "Args": [
                "flink",
                "run",
                "-m",
                "yarn-cluster",
                "/usr/lib/flink/examples/batch/WordCount.jar"
            ]
        }
    ]' \
    --region eu-central-1 \
    --profile oversea1
```



## 第五部分：通过AWS控制台来创建集群和提交jobs (不用AWSCLI方式)

### 5.1 通过AWS控制台来创建集群

![image-20250825102052377](./assets/image-20250825102052377.png)

![image-20250825102350378](./assets/image-20250825102350378.png)

![image-20250825102720117](./assets/image-20250825102720117.png)

![image-20250825102808307](./assets/image-20250825102808307.png)



### 5.2通过AWS控制台来提交jobs

![image-20250825103224170](./assets/image-20250825103224170.png)

![image-20250825105909927](./assets/image-20250825105909927.png)

##### **命令行任务的成果，用手动COPY过来使用，从而学习如何提交任务：**

![image-20250825105646901](./assets/image-20250825105646901.png)



### 

## 第六部分：Flink应用程序 v2 -扩展为有S3输入语S3输出的Java程序

### 6.0 创建Maven项目

```bash
cd ..

# 创建项目目录
mkdir flink-app-v2 && cd flink-app-v2

# 创建Maven项目结构
mkdir -p src/main/java/com/example/flink

# 创建sample-input-data.txt
vim ./sample-input-data.txt
```

### 6.1 创建sample-input-data.txt

```
To be, or not to be, that is the question:
Whether 'tis nobler in the mind to suffer
The slings and arrows of outrageous fortune,
Or to take arms against a sea of troubles
And by opposing end them. To die—to sleep,
No more; and by a sleep to say we end
The heartache and the thousand natural shocks
That flesh is heir to: 'tis a consummation
Devoutly to be wished. To die, to sleep;
To sleep, perchance to dream—ay, there's the rub:
For in that sleep of death what dreams may come,
When we have shuffled off this mortal coil,
Must give us pause—there's the respect
That makes calamity of so long life.
For who would bear the whips and scorns of time,
The oppressor's wrong, the proud man's contumely,
The pangs of despised love, the law's delay,
The insolence of office, and the spurns
That patient merit of th' unworthy takes,
When he himself might his quietus make
With a bare bodkin? Who would fardels bear,
To grunt and sweat under a weary life,
But that the dread of something after death,
The undiscovered country from whose bourn
No traveler returns, puzzles the will
And makes us rather bear those ills we have
Than fly to others that we know not of?
Thus conscience does make cowards of us all,
And thus the native hue of resolution
Is sicklied o'er with the pale cast of thought,
And enterprises of great pith and moment
With this regard their currents turn awry
And lose the name of action.

Apache Flink is a framework and distributed processing engine for stateful computations over unbounded and bounded data streams.
Flink has been designed to run in all common cluster environments, perform computations at in-memory speed and at any scale.
Here, we explain the important aspects of Flink's architecture.

Big data processing has become an essential part of modern applications.
Stream processing enables real-time analytics and decision making.
Batch processing handles large volumes of historical data efficiently.
Flink provides unified stream and batch processing capabilities.

The future of data processing lies in unified platforms that can handle both streaming and batch workloads seamlessly.
Real-time insights drive business value in today's competitive landscape.
Scalable architectures enable organizations to grow without technical constraints.
Open source technologies democratize access to advanced data processing capabilities.

```

### 6.2 创建pom.xml

```bash
# 创建
vim pom.xml
```

**✅ 完整验证的Maven配置**:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.example</groupId>
    <artifactId>flink-emr-s3-input</artifactId>
    <version>1.0-SNAPSHOT</version>
    <packaging>jar</packaging>

    <properties>
        <maven.compiler.source>8</maven.compiler.source>
        <maven.compiler.target>8</maven.compiler.target>
        <flink.version>1.17.1</flink.version>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-java</artifactId>
            <version>${flink.version}</version>
            <scope>provided</scope>
        </dependency>
        
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-streaming-java</artifactId>
            <version>${flink.version}</version>
            <scope>provided</scope>
        </dependency>
        
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-clients</artifactId>
            <version>${flink.version}</version>
            <scope>provided</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.8.1</version>
                <configuration>
                    <source>8</source>
                    <target>8</target>
                </configuration>
            </plugin>
            
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <version>3.2.4</version>
                <executions>
                    <execution>
                        <phase>package</phase>
                        <goals>
                            <goal>shade</goal>
                        </goals>
                        <configuration>
                            <transformers>
                                <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                                    <mainClass>com.example.flink.WordCountS3Input</mainClass>
                                </transformer>
                            </transformers>
                            <filters>
                                <filter>
                                    <artifact>*:*</artifact>
                                    <excludes>
                                        <exclude>META-INF/*.SF</exclude>
                                        <exclude>META-INF/*.DSA</exclude>
                                        <exclude>META-INF/*.RSA</exclude>
                                    </excludes>
                                </filter>
                            </filters>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>

```

### 6.3 创建WordCount应用程序

**✅ 支持双输出模式的完整应用**: WordCountS3Input.java

```bash
# 创建源码在准确的项目目录（很重要，否则后面emr执行报错）
vim src/main/java/com/example/flink/WordCountS3Input.java
```

```java
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

```

### 6.4 构建应用程序

```bash
# 编译和打包
mvn clean package

# 验证JAR文件
ls -la target/flink-emr-s3-input-1.0-SNAPSHOT.jar

# 验证JAR文件 检查类是否确实包含在 JAR 中
jar tvf target/flink-emr-s3-input-1.0-SNAPSHOT.jar | grep WordCountS3Input
#  5165 Mon Aug 25 09:56:22 CST 2025 com/example/flink/WordCountS3Input.class
#  2032 Mon Aug 25 09:56:22 CST 2025 com/example/flink/WordCountS3Input$Tokenizer.class
```

### 6.5 上传JAR到S3

```bash
# 上传应用程序JAR
aws s3 cp target/flink-emr-s3-input-1.0-SNAPSHOT.jar \
    s3://$BUCKET_NAME/jars/wordcount-s3-input-output-app.jar \
    --profile oversea1

# 上传输入的TXT文本到S3   
aws s3 cp ./sample-input-data.txt \
    s3://$BUCKET_NAME/input/ \
    --profile oversea1
```

### 6.6 S3输入/输出模式 

```bash
# 运行S3输出版本
aws emr add-steps \
    --cluster-id $CLUSTER_ID \
    --steps '[
        {
            "Name": "Flink-WordCount-S3-input-Output",
            "ActionOnFailure": "CONTINUE",
            "Jar": "command-runner.jar",
            "Args": [
                "bash", "-c",
                "aws s3 cp s3://'$BUCKET_NAME'/jars/wordcount-s3-input-output-app.jar /tmp/app.jar && flink run -m yarn-cluster /tmp/app.jar s3://'$BUCKET_NAME'/input/ s3://'$BUCKET_NAME'/output/"
            ]
        }
    ]' \
    --region eu-central-1 \
    --profile oversea1
```

**预期结果**:

- 处理结果: 更多N个单词的词频统计
- 输入位置: `s3://bucket/input/sample-input-data.txt`
- 输出位置: `s3://bucket/output/wordcount-results`
- 文件大小: ~1.7KB

![image-20250825120701444](./assets/image-20250825120701444.png)

![image-20250825121757574](./assets/image-20250825121757574.png)



## 第七部分：监控和验证

### 5.1 监控作业状态

```bash
# 获取步骤ID (从作业提交输出中获取)
STEP_ID="s-XXXXXXXXXX"

# 持续监控
while true; do
    STATUS=$(aws emr describe-step --cluster-id $CLUSTER_ID --step-id $STEP_ID --query 'Step.Status.State' --output text --region eu-central-1 --profile oversea1)
    echo "作业状态: $STATUS"
    if [ "$STATUS" = "COMPLETED" ]; then
        echo "作业成功完成!"
        break
    elif [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "CANCELLED" ]; then
        echo "作业失败!"
        break
    fi
    sleep 10
done
```



### 5.2 验证S3输出结果

```bash
# 检查S3输出文件
aws s3 ls s3://$BUCKET_NAME/output/ --recursive --profile oversea1

# 下载并查看结果
aws s3 cp s3://$BUCKET_NAME/output/wordcount-results /tmp/results.txt --profile oversea1
head -10 /tmp/results.txt

# 统计结果
wc -l /tmp/results.txt
```

**预期输出示例**:
```
(a,5)
(action,1)
(after,1)
(against,1)
(all,2)
(and,12)
(arms,1)
(arrows,1)
(awry,1)
(ay,1)
...
169 /tmp/results.txt
```

### 5.3 查看作业日志

```bash
# 等待日志上传
sleep 60

# 下载并查看日志
aws s3 cp s3://$BUCKET_NAME/logs/$CLUSTER_ID/steps/$STEP_ID/stdout.gz /tmp/ --profile oversea1
gunzip -c /tmp/stdout.gz | tail -20
```



## 第八部分：故障排除

### 6.1 常见问题及解决方案

#### 问题1：数据sink执行顺序错误
**症状**: "No new data sinks have been defined since the last execution"
**解决方案**: 
- 控制台模式：仅使用`print()`，不调用`execute()`
- S3模式：仅使用`writeAsText()`，然后调用`execute()`

#### 问题2：S3路径问题
**症状**: "JAR file does not exist" 或路径解析错误
**解决方案**: 
- 使用HDFS中转：先复制到本地再执行
- 确保S3路径格式正确：`s3://bucket/path/`

#### 问题3：权限问题
**症状**: S3访问被拒绝
**解决方案**: 
- 确认EMR_EC2_DefaultRole有S3访问权限
- 检查存储桶策略和IAM角色配置



### 6.2 性能优化建议

#### 资源配置优化
```json
{
    "Classification": "flink-conf",
    "Properties": {
        "taskmanager.memory.process.size": "4096m",
        "jobmanager.memory.process.size": "2048m",
        "taskmanager.numberOfTaskSlots": "4",
        "parallelism.default": "4"
    }
}
```

#### YARN配置优化
```json
{
    "Classification": "yarn-site",
    "Properties": {
        "yarn.nodemanager.resource.memory-mb": "6144",
        "yarn.scheduler.maximum-allocation-mb": "6144"
    }
}
```



Flink on EMR

<https://docs.amazonaws.cn/emr/latest/ReleaseGuide/flink-create-cluster.html>

使用 Apache Flink 在 Amazon EMR 上构建统一数据湖

<https://aws.amazon.com/cn/blogs/china/build-a-unified-data-lake-with-apache-flink-on-amazon-emr/>





## 第九部分：清理资源

### 7.1 终止EMR集群

```bash
# 终止集群
aws emr terminate-clusters \
    --cluster-ids $CLUSTER_ID \
    --region eu-central-1 \
    --profile oversea1
```

### 7.2 清理S3资源

```bash
# 删除S3存储桶内容
aws s3 rm s3://$BUCKET_NAME --recursive --profile oversea1

# 删除存储桶
aws s3 rb s3://$BUCKET_NAME --profile oversea1
```

### 7.3 清理其他资源

```bash
# 删除密钥对
aws ec2 delete-key-pair \
    --key-name flink-emr-keypair \
    --region eu-central-1 \
    --profile oversea1

# 删除本地文件
rm -f flink-emr-keypair.pem cluster_id.txt bucket_name.txt
```



## 附录

### A. 验证数据参考

#### 实际运行性能指标
- **集群启动时间**: 7分钟
- **控制台输出作业**: 24秒 (包含环境准备)
- **S3输出作业**: 28秒 (包含文件写入)
- **Flink执行时间**: 13.343秒 (纯处理时间)
- **处理数据量**: 169个单词
- **输出文件大小**: 1.7KB

#### 预期输出格式
```
(the,22) (to,15) (of,15) (and,12) (that,7) (a,5) (s,5) (sleep,5)
(be,4) (we,4) (us,4) (d,4) (in,3) (is,3) (with,3) (all,2) (by,2)
...
```

### C. 最佳实践总结

1. **开发阶段**: 使用控制台输出模式快速验证
2. **测试阶段**: 使用S3输出模式验证持久化
3. **生产阶段**: 配置合适的并行度和资源
4. **监控**: 定期检查作业状态和输出结果
5. **成本控制**: 及时清理不需要的资源



## 第十部分：总结

本完整正确版手册基于EMR集群 `` 的实际验证，实现了：

✅ **双输出模式**: 控制台和S3输出都完全正常
✅ **问题解决**: 修正了数据sink执行顺序问题
✅ **完整验证**: 所有功能都经过实际测试
✅ **生产就绪**: 可直接用于生产环境部署

**推荐使用顺序**: 内置示例验证环境 → 控制台输出测试 → S3输出生产

**状态**: ✅ **100%验证通过** - 可放心用于各种环境部署





## 附件1：Flink应用程序架构对比分析

#### 概述

本文档对比分析了AWS Flink on YARN实验手册中的两个Flink应用程序的架构设计，帮助学员理解不同应用场景下的技术选择和架构差异。

#### 应用程序对比表

| 维度           | Flink应用程序V1          | Flink应用程序V2             |
| -------------- | ------------------------ | --------------------------- |
| **应用名称**   | WordCountWithS3Output    | WordCountS3Input            |
| **主要特性**   | 内置数据源，双输出模式   | S3输入输出，生产级处理      |
| **数据源类型** | 内置文本数据             | S3存储文件                  |
| **输入方式**   | `env.fromElements()`     | `env.readTextFile()`        |
| **数据内容**   | 莎士比亚《哈姆雷特》独白 | 《哈姆雷特》+ Flink技术文档 |
| **数据量**     | 35行文本，169个单词      | 40+行文本，更多词汇         |
| **参数要求**   | 可选S3输出路径           | 必需输入路径，可选输出路径  |
| **错误处理**   | 基础异常处理             | 完整的参数验证和异常处理    |
| **日志记录**   | 简单状态日志             | 详细的执行日志              |
| **执行时间**   | 24-28秒                  | 30-35秒                     |
| **适用场景**   | 学习、演示、快速验证     | 生产环境、ETL作业           |
| **生产就绪度** | 演示级别                 | 生产级别                    |

#### 架构设计对比

#### 1. 数据流架构

#### V1版本 - 内置数据源架构

```
内置数据 → FlatMap → GroupBy/Sum → 控制台/S3输出
```

**特点**:

- 数据源固定，无外部依赖
- 处理流程简单直接
- 适合快速验证和学习

#### V2版本 - S3输入输出架构

```
S3输入 → 参数验证 → FlatMap → GroupBy/Sum → 控制台/S3输出
```

**特点**:

- 支持动态数据源
- 包含完整的错误处理
- 更接近生产环境需求

#### 2. 技术架构层次

#### 共同的基础架构

```
┌─────────────────────────────────────┐
│           AWS EMR Cluster           │
│  ┌─────────────────────────────────┐ │
│  │        Hadoop YARN              │ │
│  │  ┌─────────────────────────────┐ │ │
│  │  │      Apache Flink           │ │ │
│  │  │  ┌─────────────────────────┐ │ │ │
│  │  │  │    JobManager           │ │ │ │
│  │  │  └─────────────────────────┘ │ │ │
│  │  │  ┌─────────────────────────┐ │ │ │
│  │  │  │   TaskManager 1         │ │ │ │
│  │  │  └─────────────────────────┘ │ │ │
│  │  │  ┌─────────────────────────┐ │ │ │
│  │  │  │   TaskManager 2         │ │ │ │
│  │  │  └─────────────────────────┘ │ │ │
│  │  └─────────────────────────────┘ │ │
│  └─────────────────────────────────┘ │
└─────────────────────────────────────┘
```

#### V1版本特有组件

```
┌─────────────────────────────────────┐
│         JobManager                  │
│  ┌─────────────────────────────────┐ │
│  │      内置数据源                  │ │
│  │   (莎士比亚《哈姆雷特》独白)      │ │
│  └─────────────────────────────────┘ │
└─────────────────────────────────────┘
```

#### V2版本特有组件

```
┌─────────────────────────────────────┐
│         JobManager                  │
│  ┌─────────────────────────────────┐ │
│  │      S3 Reader                  │ │
│  │   (动态文件读取)                 │ │
│  └─────────────────────────────────┘ │
│  ┌─────────────────────────────────┐ │
│  │    参数验证器                    │ │
│  │   (输入路径验证)                 │ │
│  └─────────────────────────────────┘ │
└─────────────────────────────────────┘
```

#### 3. 存储架构对比

#### V1版本存储架构

```
Amazon S3 (flink-emr-lab-bucket)
├── jars/
│   └── wordcount-s3-app.jar
├── output/                    # 可选输出
│   └── wordcount-results
└── logs/
    └── EMR集群日志
```

#### V2版本存储架构

```
Amazon S3 (flink-emr-lab-bucket)
├── input/                     # 必需输入
│   └── sample-input-data.txt
├── jars/
│   └── wordcount-s3-input-output-app.jar
├── output/                    # 可选输出
│   └── wordcount-results
└── logs/
    └── EMR集群日志
```

#### 代码架构对比

#### 1. 主函数结构

#### V1版本主函数

```java
public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    
    // 内置数据源
    DataSet<String> text = env.fromElements(/* 硬编码文本 */);
    
    // 处理逻辑
    DataSet<Tuple2<String, Integer>> counts = text
        .flatMap(new Tokenizer())
        .groupBy(0)
        .sum(1);
    
    // 输出选择
    if (args.length > 0) {
        // S3输出
        counts.writeAsText(outputPath);
        env.execute();
    } else {
        // 控制台输出
        counts.print();
    }
}
```

#### V2版本主函数

```java
public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    
    // 参数验证
    if (args.length < 1) {
        System.err.println("错误: 缺少输入路径参数");
        System.exit(1);
    }
    
    try {
        // S3数据源
        DataSet<String> text = env.readTextFile(inputPath);
        
        // 处理逻辑
        DataSet<Tuple2<String, Integer>> counts = text
            .flatMap(new Tokenizer())
            .groupBy(0)
            .sum(1);
        
        // 输出选择
        if (outputPath != null) {
            counts.writeAsText(outputPath);
            env.execute();
        } else {
            counts.print();
        }
    } catch (Exception e) {
        // 完整错误处理
        System.err.println("作业执行失败: " + e.getMessage());
        throw e;
    }
}
```

#### 技术演进路径

#### 1. 从V1到V2的演进

```
V1 (学习版) → V2 (生产版) → 企业级应用
     ↓              ↓              ↓
   概念验证      功能完整        规模化部署
   快速测试      错误处理        监控告警
   环境验证      参数验证        性能优化
```

#### 2. 进一步扩展方向

#### 数据源扩展

- **多格式支持**: JSON, CSV, Parquet
- **流式数据**: Kafka, Kinesis
- **数据库**: RDS, DynamoDB

#### 处理能力扩展

- **复杂分析**: 窗口函数, 状态管理
- **机器学习**: FlinkML集成
- **实时处理**: 流批一体化

#### 运维能力扩展

- **监控集成**: CloudWatch, Prometheus
- **告警机制**: SNS, Lambda
- **自动扩缩**: EMR Auto Scaling

#### 最佳实践建议

### 1. 选择指导原则

#### 选择V1版本的场景

- Flink初学者学习
- EMR环境快速验证
- 概念原型开发
- 功能演示展示

#### 选择V2版本的场景

- 生产环境部署
- ETL作业开发
- 大数据处理项目
- 企业级应用开发

#### 总结

两个Flink应用程序版本各有特色，V1版本适合学习和快速验证，V2版本适合生产环境部署。通过对比分析，学员可以：

1. **理解架构演进**: 从简单到复杂的技术演进路径
2. **掌握设计原则**: 不同场景下的架构设计考虑
3. **学习最佳实践**: 生产级应用的开发规范
4. **规划技术路线**: 从学习到生产的技术成长路径

建议学员按照V1→V2的顺序进行学习，先掌握基础概念，再深入生产级应用开发。



## 附件2：Flink应用程序V1架构图 - WordCountWithS3Output

#### 应用程序概述

**应用名称**: WordCountWithS3Output  
**主要特性**: 内置数据源，支持双输出模式  
**适用场景**: 快速验证、测试、演示  

####  AWS架构图

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                                    AWS Cloud                                        │
│                                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │                              Amazon EMR Cluster                             │   │
│  │                            (j-PSJC020KDGT6)                                │   │
│  │                                                                             │   │
│  │  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐        │   │
│  │  │   Master Node   │    │   Core Node 1   │    │   Core Node 2   │        │   │
│  │  │   r6g.xlarge    │    │   r6g.xlarge    │    │   r6g.xlarge    │        │   │
│  │  │                 │    │                 │    │                 │        │   │
│  │  │ ┌─────────────┐ │    │ ┌─────────────┐ │    │ ┌─────────────┐ │        │   │
│  │  │ │   Hadoop    │ │    │ │   Hadoop    │ │    │ │   Hadoop    │ │        │   │
│  │  │ │   YARN      │ │    │ │   YARN      │ │    │ │   YARN      │ │        │   │
│  │  │ │   Flink     │ │    │ │   Flink     │ │    │ │   Flink     │ │        │   │
│  │  │ │   Zeppelin  │ │    │ │             │ │    │ │             │ │        │   │
│  │  │ └─────────────┘ │    │ └─────────────┘ │    │ └─────────────┘ │        │   │
│  │  └─────────────────┘    └─────────────────┘    └─────────────────┘        │   │
│  │                                                                             │   │
│  │  ┌─────────────────────────────────────────────────────────────────────┐   │   │
│  │  │                    Flink on YARN 应用执行                           │   │   │
│  │  │                                                                     │   │   │
│  │  │  ┌─────────────────┐                                               │   │   │
│  │  │  │   JobManager    │                                               │   │   │
│  │  │  │   (8GB Memory)  │                                               │   │   │
│  │  │  │                 │                                               │   │   │
│  │  │  │ ┌─────────────┐ │                                               │   │   │
│  │  │  │ │ 内置数据源   │ │                                               │   │   │
│  │  │  │ │《哈姆雷特》  │ │                                               │   │   │
│  │  │  │ │独白文本     │ │                                               │   │   │
│  │  │  │ └─────────────┘ │                                               │   │   │
│  │  │  └─────────────────┘                                               │   │   │
│  │  │           │                                                        │   │   │
│  │  │           ▼                                                        │   │   │
│  │  │  ┌─────────────────┐    ┌─────────────────┐                       │   │   │
│  │  │  │  TaskManager 1  │    │  TaskManager 2  │                       │   │   │
│  │  │  │  (20GB Memory)  │    │  (20GB Memory)  │                       │   │   │
│  │  │  │  4 Task Slots   │    │  4 Task Slots   │                       │   │   │
│  │  │  │                 │    │                 │                       │   │   │
│  │  │  │ ┌─────────────┐ │    │ ┌─────────────┐ │                       │   │   │
│  │  │  │ │  FlatMap    │ │    │ │  FlatMap    │ │                       │   │   │
│  │  │  │ │ (Tokenizer) │ │    │ │ (Tokenizer) │ │                       │   │   │
│  │  │  │ └─────────────┘ │    │ └─────────────┘ │                       │   │   │
│  │  │  │ ┌─────────────┐ │    │ ┌─────────────┐ │                       │   │   │
│  │  │  │ │   GroupBy   │ │    │ │   GroupBy   │ │                       │   │   │
│  │  │  │ │    Sum      │ │    │ │    Sum      │ │                       │   │   │
│  │  │  │ └─────────────┘ │    │ └─────────────┘ │                       │   │   │
│  │  │  └─────────────────┘    └─────────────────┘                       │   │   │
│  │  └─────────────────────────────────────────────────────────────────────┐   │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │   │
│                                                                                 │   │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │   │
│  │                           Amazon S3                                     │   │   │
│  │                    (flink-emr-lab-bucket)                              │   │   │
│  │                                                                         │   │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐   │   │   │
│  │  │    jars/    │  │   output/   │  │    logs/    │  │checkpoints/ │   │   │   │
│  │  │             │  │             │  │             │  │             │   │   │   │
│  │  │ wordcount-  │  │ wordcount-  │  │   EMR集群   │  │   (备用)    │   │   │   │
│  │  │ s3-app.jar  │  │ results     │  │   日志      │  │             │   │   │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘   │   │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │   │
│                                                                                 │   │
└─────────────────────────────────────────────────────────────────────────────────┘   │
                                                                                     │
┌─────────────────────────────────────────────────────────────────────────────────┐   │
│                              数据流向图                                          │   │
│                                                                                 │   │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐     │   │
│  │  内置数据源  │───▶│   分词处理   │───▶│   词频统计   │───▶│   输出结果   │     │   │
│  │             │    │             │    │             │    │             │     │   │
│  │《哈姆雷特》  │    │  FlatMap    │    │  GroupBy    │    │ 控制台/S3   │     │   │
│  │独白文本     │    │ (Tokenizer) │    │    Sum      │    │             │     │   │
│  │(35行文本)   │    │             │    │             │    │ (169个词)   │     │   │
│  └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘     │   │
│                                                                                 │   │
└─────────────────────────────────────────────────────────────────────────────────┘   │
```

####  技术架构详解

#### 1. EMR集群配置

- **集群类型**: EMR 6.15.0
- **实例类型**: r6g.xlarge (ARM Graviton处理器)
- **节点配置**: 1个Master节点 + 2个Core节点
- **内存配置**: 
  - JobManager: 8GB
  - TaskManager: 20GB (每节点)
  - Task Slots: 4个 (每TaskManager)

#### 2. 应用程序特性

- **数据源**: 内置文本数据（莎士比亚《哈姆雷特》独白）
- **处理逻辑**: 标准WordCount算法
- **输出模式**: 
  - 控制台输出（调试模式）
  - S3输出（生产模式）

#### 3. 数据处理流程

1. **数据输入**: 从内置数据源读取35行文本
2. **分词处理**: 使用FlatMap算子进行分词
3. **词频统计**: 使用GroupBy和Sum算子统计词频
4. **结果输出**: 输出169个单词的词频统计

#### 4. 执行模式对比

#### 控制台输出模式

```bash
flink run -m yarn-cluster /tmp/app.jar
```

- **执行时间**: ~24秒
- **输出位置**: 作业日志stdout
- **适用场景**: 开发测试、快速验证

#### S3输出模式

```bash
flink run -m yarn-cluster /tmp/app.jar s3://bucket/output/
```

- **执行时间**: ~28秒
- **输出位置**: S3存储桶
- **文件大小**: ~1.7KB
- **适用场景**: 生产环境、数据持久化

#### 5. 关键配置参数

#### Flink配置

```json
{
  "taskmanager.memory.process.size": "20480m",
  "jobmanager.memory.process.size": "8192m",
  "taskmanager.numberOfTaskSlots": "4",
  "classloader.check-leaked-classloader": "false"
}
```

#### YARN配置

```json
{
  "yarn.nodemanager.resource.memory-mb": "24576",
  "yarn.nodemanager.vmem-check-enabled": "false"
}
```

####  验证结果

#### 性能指标

- **集群启动时间**: 7分钟
- **作业执行时间**: 13.343秒（纯Flink处理时间）
- **总处理时间**: 24-28秒（包含环境准备）
- **处理数据量**: 169个唯一单词
- **输出文件大小**: 1.7KB

#### 输出示例

```
(the,22) (to,15) (of,15) (and,12) (that,7) (a,5) (s,5) (sleep,5)
(be,4) (we,4) (us,4) (d,4) (in,3) (is,3) (with,3) (all,2) (by,2)
...
```

####  优势与适用场景

#### 优势

1. **快速验证**: 无需准备输入数据，可立即运行
2. **双输出模式**: 支持开发和生产两种场景
3. **完整示例**: 包含完整的WordCount实现
4. **易于理解**: 代码结构清晰，注释详细

#### 适用场景

1. **学习演示**: Flink基础概念学习
2. **环境验证**: EMR集群功能验证
3. **快速测试**: 新配置或代码的快速验证
4. **原型开发**: 作为其他应用的开发基础

####  扩展建议

1. **增加数据源**: 可扩展为从文件或流式数据源读取
2. **复杂处理**: 可添加更复杂的数据处理逻辑
3. **状态管理**: 可添加检查点和状态后端配置
4. **监控告警**: 可集成CloudWatch进行监控



## 附件3：Flink应用程序V2架构图 - WordCountS3Input

#### 应用程序概述

**应用名称**: WordCountS3Input  
**主要特性**: S3输入输出，生产级数据处理  
**适用场景**: 生产环境、大数据处理、ETL作业  

#### AWS架构图

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                                    AWS Cloud                                        │
│                                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │                              Amazon EMR Cluster                             │   │
│  │                            (j-PSJC020KDGT6)                                │   │
│  │                                                                             │   │
│  │  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐        │   │
│  │  │   Master Node   │    │   Core Node 1   │    │   Core Node 2   │        │   │
│  │  │   r6g.xlarge    │    │   r6g.xlarge    │    │   r6g.xlarge    │        │   │
│  │  │                 │    │                 │    │                 │        │   │
│  │  │ ┌─────────────┐ │    │ ┌─────────────┐ │    │ ┌─────────────┐ │        │   │
│  │  │ │   Hadoop    │ │    │ │   Hadoop    │ │    │ │   Hadoop    │ │        │   │
│  │  │ │   YARN      │ │    │ │   YARN      │ │    │ │   YARN      │ │        │   │
│  │  │ │   Flink     │ │    │ │   Flink     │ │    │ │   Flink     │ │        │   │
│  │  │ │   Zeppelin  │ │    │ │             │ │    │ │             │ │        │   │
│  │  │ └─────────────┘ │    │ └─────────────┘ │    │ └─────────────┘ │        │   │
│  │  └─────────────────┘    └─────────────────┘    └─────────────────┘        │   │
│  │                                                                             │   │
│  │  ┌─────────────────────────────────────────────────────────────────────┐   │   │
│  │  │                    Flink on YARN 应用执行                           │   │   │
│  │  │                                                                     │   │   │
│  │  │  ┌─────────────────┐                                               │   │   │
│  │  │  │   JobManager    │                                               │   │   │
│  │  │  │   (8GB Memory)  │                                               │   │   │
│  │  │  │                 │                                               │   │   │
│  │  │  │ ┌─────────────┐ │                                               │   │   │
│  │  │  │ │ S3 Reader   │ │◀──────────────────────────────────────────┐   │   │   │
│  │  │  │ │ 输入数据源   │ │                                           │   │   │   │
│  │  │  │ └─────────────┘ │                                           │   │   │   │
│  │  │  └─────────────────┘                                           │   │   │   │
│  │  │           │                                                    │   │   │   │
│  │  │           ▼                                                    │   │   │   │
│  │  │  ┌─────────────────┐    ┌─────────────────┐                   │   │   │   │
│  │  │  │  TaskManager 1  │    │  TaskManager 2  │                   │   │   │   │
│  │  │  │  (20GB Memory)  │    │  (20GB Memory)  │                   │   │   │   │
│  │  │  │  4 Task Slots   │    │  4 Task Slots   │                   │   │   │   │
│  │  │  │                 │    │                 │                   │   │   │   │
│  │  │  │ ┌─────────────┐ │    │ ┌─────────────┐ │                   │   │   │   │
│  │  │  │ │  FlatMap    │ │    │ │  FlatMap    │ │                   │   │   │   │
│  │  │  │ │ (Tokenizer) │ │    │ │ (Tokenizer) │ │                   │   │   │   │
│  │  │  │ └─────────────┘ │    │ └─────────────┘ │                   │   │   │   │
│  │  │  │ ┌─────────────┐ │    │ ┌─────────────┐ │                   │   │   │   │
│  │  │  │ │   GroupBy   │ │    │ │   GroupBy   │ │                   │   │   │   │
│  │  │  │ │    Sum      │ │    │ │    Sum      │ │                   │   │   │   │
│  │  │  │ └─────────────┘ │    │ └─────────────┘ │                   │   │   │   │
│  │  │  │ ┌─────────────┐ │    │ ┌─────────────┐ │                   │   │   │   │
│  │  │  │ │ S3 Writer   │ │    │ │ S3 Writer   │ │                   │   │   │   │
│  │  │  │ │ 输出数据源   │ │    │ │ 输出数据源   │ │                   │   │   │   │
│  │  │  │ └─────────────┘ │    │ └─────────────┘ │                   │   │   │   │
│  │  │  └─────────────────┘    └─────────────────┘                   │   │   │   │
│  │  │           │                       │                          │   │   │   │
│  │  │           └───────────────────────┼──────────────────────────┘   │   │   │
│  │  └─────────────────────────────────────────────────────────────────────┐   │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │   │
│                                         │                                       │   │
│                                         ▼                                       │   │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │   │
│  │                           Amazon S3                                     │   │   │
│  │                    (flink-emr-lab-bucket)                              │   │   │
│  │                                                                         │   │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐   │   │   │
│  │  │   input/    │  │    jars/    │  │   output/   │  │    logs/    │   │   │   │
│  │  │             │  │             │  │             │  │             │   │   │   │
│  │  │ sample-     │  │ wordcount-  │  │ wordcount-  │  │   EMR集群   │   │   │   │
│  │  │ input-      │  │ s3-input-   │  │ results     │  │   日志      │   │   │   │
│  │  │ data.txt    │  │ output-     │  │             │  │             │   │   │   │
│  │  │             │  │ app.jar     │  │             │  │             │   │   │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘   │   │   │
│  │         ▲                                   ▲                         │   │   │
│  │         │                                   │                         │   │   │
│  │         └───────────────────────────────────┘                         │   │   │
│  │                        数据流向                                        │   │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │   │
│                                                                                 │   │
└─────────────────────────────────────────────────────────────────────────────────┘   │
                                                                                     │
┌─────────────────────────────────────────────────────────────────────────────────┐   │
│                              数据流向图                                          │   │
│                                                                                 │   │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐     │   │
│  │  S3输入数据  │───▶│   分词处理   │───▶│   词频统计   │───▶│  S3输出结果  │     │   │
│  │             │    │             │    │             │    │             │     │   │
│  │sample-input-│    │  FlatMap    │    │  GroupBy    │    │ wordcount-  │     │   │
│  │data.txt     │    │ (Tokenizer) │    │    Sum      │    │ results     │     │   │
│  │             │    │             │    │             │    │             │     │   │
│  │《哈姆雷特》+ │    │ 文本分词     │    │ 单词计数     │    │ 词频统计     │     │   │
│  │Flink技术文档│    │ 去重处理     │    │ 聚合计算     │    │ 结果文件     │     │   │
│  │(40+行文本)  │    │             │    │             │    │ (1.7KB)     │     │   │
│  └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘     │   │
│                                                                                 │   │
└─────────────────────────────────────────────────────────────────────────────────┘   │
```

#### 技术架构详解

#### 1. EMR集群配置

- **集群类型**: EMR 6.15.0
- **实例类型**: r6g.xlarge (ARM Graviton处理器)
- **节点配置**: 1个Master节点 + 2个Core节点
- **内存配置**: 
  - JobManager: 8GB
  - TaskManager: 20GB (每节点)
  - Task Slots: 4个 (每TaskManager)

#### 2. 应用程序特性

- **数据源**: S3存储的文本文件
- **输入文件**: sample-input-data.txt (包含《哈姆雷特》独白 + Flink技术文档)
- **处理逻辑**: 增强版WordCount算法
- **输出模式**: 
  - 控制台输出（调试模式）
  - S3输出（生产模式）

#### 3. 数据处理流程

1. **数据读取**: 从S3读取输入文件 (`s3://bucket/input/sample-input-data.txt`)
2. **数据验证**: 检查输入参数和文件有效性
3. **分词处理**: 使用增强的FlatMap算子进行分词和清洗
4. **词频统计**: 使用GroupBy和Sum算子统计词频
5. **结果输出**: 输出到S3或控制台

#### 4. S3存储结构

#### 输入数据 (input/)

```
s3://flink-emr-lab-bucket/input/
├── sample-input-data.txt    # 输入文本文件
└── (其他输入文件...)
```

#### 应用程序 (jars/)

```
s3://flink-emr-lab-bucket/jars/
├── wordcount-s3-input-output-app.jar    # 应用程序JAR
└── (其他JAR文件...)
```

#### 输出结果 (output/)

```
s3://flink-emr-lab-bucket/output/
├── wordcount-results/       # 词频统计结果
│   └── part-00000          # 结果文件
└── (其他输出文件...)
```

#### 5. 执行模式对比

#### 控制台输出模式

```bash
flink run -m yarn-cluster /tmp/app.jar s3://bucket/input/
```

- **参数**: 仅输入路径
- **输出位置**: 作业日志stdout
- **适用场景**: 开发测试、数据验证

#### S3输出模式

```bash
flink run -m yarn-cluster /tmp/app.jar s3://bucket/input/ s3://bucket/output/
```

- **参数**: 输入路径 + 输出路径
- **输出位置**: S3存储桶
- **文件格式**: 文本文件，每行一个词频对
- **适用场景**: 生产环境、数据持久化

#### 6. 关键代码特性

#### 参数验证

```java
if (args.length < 1) {
    System.err.println("错误: 缺少输入路径参数");
    System.exit(1);
}
```

#### S3数据读取

```java
DataSet<String> text = env.readTextFile(inputPath);
```

#### 增强分词器

```java
public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
        if (value == null || value.trim().isEmpty()) {
            return; // 跳过空行
        }
        String[] words = value.toLowerCase().split("\\W+");
        for (String word : words) {
            if (word.length() > 0) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
```

#### 7. 错误处理和日志

#### 完整的错误处理

```java
try {
    // 主要处理逻辑
    DataSet<String> text = env.readTextFile(inputPath);
    // ... 处理逻辑
    env.execute("WordCount S3 Input to S3 Output");
} catch (Exception e) {
    System.err.println("=== WordCount作业执行失败 ===");
    System.err.println("错误信息: " + e.getMessage());
    e.printStackTrace();
    throw e;
}
```

#### 详细的日志记录

```java
System.out.println("=== WordCount S3输入版本启动 ===");
System.out.println("输入路径: " + inputPath);
System.out.println("输出路径: " + (outputPath != null ? outputPath : "控制台"));
```

#### 验证结果

#### 性能指标

- **数据读取时间**: ~2-3秒（从S3读取）
- **作业执行时间**: ~15-20秒（纯Flink处理时间）
- **总处理时间**: ~30-35秒（包含S3 I/O）
- **处理数据量**: 更多单词（包含技术文档）
- **输出文件大小**: ~1.7KB

#### 输入数据特性

- **文件大小**: ~2KB
- **行数**: 40+行
- **内容**: 《哈姆雷特》独白 + Apache Flink技术文档
- **词汇丰富度**: 更高的词汇多样性

#### 输出示例

```
(the,25) (to,18) (of,17) (and,15) (data,8) (flink,6) (processing,5)
(stream,4) (batch,3) (real,3) (time,3) (applications,2) (analytics,2)
...
```

#### 优势与适用场景

#### 优势

1. **生产就绪**: 完整的S3输入输出支持
2. **错误处理**: 完善的异常处理和日志记录
3. **参数验证**: 严格的输入参数验证
4. **灵活输出**: 支持控制台和S3双输出模式
5. **可扩展性**: 易于扩展为更复杂的数据处理

#### 适用场景

1. **生产环境**: 大规模数据处理作业
2. **ETL作业**: 数据提取、转换、加载
3. **批处理**: 定期的数据分析任务
4. **数据湖**: 数据湖中的数据处理
5. **原型验证**: 生产级应用的原型开发

#### 与V1版本对比

| 特性     | V1版本 (内置数据源) | V2版本 (S3输入输出) |
| -------- | ------------------- | ------------------- |
| 数据源   | 内置文本数据        | S3存储文件          |
| 输入方式 | 硬编码数据          | 动态文件读取        |
| 参数要求 | 可选参数            | 必需输入路径        |
| 错误处理 | 基础处理            | 完整异常处理        |
| 日志记录 | 简单日志            | 详细日志            |
| 生产就绪 | 演示级别            | 生产级别            |
| 扩展性   | 有限                | 高度可扩展          |
| 适用场景 | 学习、测试          | 生产、ETL           |

#### 扩展建议

#### 1. 数据格式支持

- **JSON处理**: 添加JSON数据解析
- **CSV处理**: 支持结构化数据
- **Parquet支持**: 高效的列式存储格式

#### 2. 流处理扩展

```java
// 扩展为流处理应用
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
DataStream<String> stream = env.readTextFile(inputPath);
```

#### 3. 状态管理

```java
// 添加检查点配置
env.enableCheckpointing(60000); // 每分钟检查点
env.getCheckpointConfig().setCheckpointStorage("s3://bucket/checkpoints/");
```

#### 4. 监控集成

```java
// 添加指标收集
env.getConfig().setLatencyTrackingInterval(1000);
```

#### 5. 安全增强

- **加密**: S3数据加密
- **访问控制**: IAM角色细粒度权限
- **审计**: CloudTrail日志记录

#### 最佳实践

#### 1. 资源配置

- 根据数据量调整TaskManager内存
- 合理设置并行度
- 配置合适的检查点间隔

#### 2. S3优化

- 使用合适的文件分区策略
- 启用S3传输加速
- 配置生命周期策略

#### 3. 错误恢复

- 配置重启策略
- 设置合理的超时时间
- 实现幂等性处理

#### 4. 性能调优

- 使用RocksDB状态后端
- 启用增量检查点
- 优化序列化配置
