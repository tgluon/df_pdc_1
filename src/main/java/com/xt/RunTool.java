package com.xt;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Scanner;

public class RunTool {
	public static long no = 1;

	public static void main(String[] args) {
		String inputFilePath = "C:\\user_score.csv";
//		String outputDirPath = "C:\\user_score_result";
		String sql = "";
		SparkConf conf = new SparkConf().setAppName("game").setMaster("local[1]").set("spark.executor.memory", "512m");
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("ERROR");
		SparkSession spark = SparkSession.builder().appName("game").getOrCreate();

		JavaRDD<String> lines = sc.textFile(inputFilePath);
		JavaRDD<User> usersRdd = lines.map(new Function<String, User>() {
			private static final long serialVersionUID = 1L;
			String[] parts = null;
			User user = null;

			public User call(String line) throws Exception {
				parts = line.split(",");
				if (parts.length == 2) {
					user = new User();
					user.setUserid(parts[0]);
					user.setScore(Long.parseLong(parts[1]));
					user.setNo(RunTool.no);
					RunTool.no++;
				}
				return user;
			}
		});
		Dataset<Row> df = spark.createDataFrame(usersRdd, User.class);
		SQLContext sqlContext = new SQLContext(spark);
		sqlContext.registerDataFrameAsTable(df, "user");

		Scanner scanner = new Scanner(System.in);
		print();
		while (scanner.hasNext()) {
			String line = scanner.nextLine();
			if ("1".equals(line)) {
				System.out.print("请输入UserId:");
				line = scanner.nextLine();
				sql = "select rank from ( select no , userid, score ,row_number() over(order by score desc,no asc)  rank from user) t where userid='"
						+ line + "'";
			} else if ("2".equals(line)) {
				System.out.println("Top 100 的信用评分的用户。");
				sql = "SELECT userid ,score,no FROM user ORDER BY score desc , no asc LIMIT 100";
			} else if ("3".equals(line)) {
				System.out.println("信用评分重复次数最高的10个信用评分。");
				sql = "SELECT score , cnt from (SELECT count(*) cnt,score FROM user GROUP BY score ) t ORDER BY cnt desc,score desc LIMIT 10";
			} else if ("4".equals(line)) {
				System.out.println("统计数据总数。");
				sql = "select count(*) from user";
			}else if ("5".equals(line)) {
				System.out.print("请输入sql:");
				line = scanner.nextLine();
				sql = line;
			} else if ("6".equals(line)) {
				break;
			}else {
				System.err.println("请输入1-6的字符！");
				print();
				continue;
			}
			System.err.println("running...");
			long startTime = System.currentTimeMillis();
			Dataset<Row> findDF = sqlContext.sql(sql);
			findDF.show(100);
			//重置行号
			RunTool.no=1l;
			//此处存储到文件是因为控制台Userid打印不全，此步耗时，先注释
			/*if ("2".equals(line)) {
				String out = outputDirPath + "\\" + line + "_" + new Date().getTime();
				System.out.println("正在输出到文件:" + out);
				findDF.javaRDD().saveAsTextFile(out);
				sql = "SELECT userid ,score,no FROM user ORDER BY score desc , no asc LIMIT 100";
			}*/
			System.out.println("");
			System.out.println("耗时：" + (System.currentTimeMillis() - startTime)/1000 +"秒");
			print();
		}
		scanner.close();
		sc.close();
		System.out.println("Thanks~");
		System.out.println("88~");
		System.exit(0);
	}

	private static void print() {
		System.out.println("");
		System.out.println("");
		System.out.println("");
		System.out.println("1、	给定用户的信用评分名次");
		System.out.println("2、	Top 100 的信用评分的用户");
		System.out.println("3、	信用评分重复次数最高的10个信用评分");
		System.out.println("4、	统计数据总数");
		System.out.println("5、	输入自定义sql");
		System.out.println("6、	退出程序");
		System.out.print("请输入序号(请输入1-6的字符)：");
	}

	public static class User implements Serializable{
		private static final long serialVersionUID = 1L;
		private Long no;//在文件中的序号
		private String userid;//用户id
		private Long score;//信用评分

		public Long getNo() {
			return no;
		}
		public void setNo(Long no) {
			this.no = no;
		}
		public String getUserid() {
			return userid;
		}
		public void setUserid(String userid) {
			this.userid = userid;
		}
		public Long getScore() {
			return score;
		}
		public void setScore(Long score) {
			this.score = score;
		}
	}

}


