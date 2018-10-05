package basic;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class Program {

	//a.jar Program / a.txt / b.txt
	public static void main(String[] args) throws IOException {
		
		if(args.length <2) {
			System.err.println("사용 방법 : Program <입력파일> <출력파일>");
			//종료에 대한 신호를 0이 아닌 [0은 정상종료임] 
			System.exit(2);
		}
		
		//일단 args의 0번째가 누구일까?
		/*
		for(int i=0; i<args.length; i++) {
			System.out.format("%d : %s\n", i, args[i]);
		}
		*/
		//hadoop fs(core-site.xml 에서 주소를 얻어서) ---> FileSystem(core-site.xml ...)
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
        
		//JAVA
		//InputStream fis = new FileInputStream("/Users/koby/test.txt");
		
		//HADOOP
		System.out.println(args[0]);
		System.out.println(args[1]);
		
		Path inputFilePath = new Path(args[0]);
		Path outFilePath = new Path(args[1]);
		
		
		System.out.println(inputFilePath);
		
		InputStream fis = hdfs.open(inputFilePath);
        Scanner fscan = new Scanner(fis);

        OutputStream fos = hdfs.create(outFilePath);
        PrintStream fout = new PrintStream(fos, true);
        
        int count = 0;
        while(fscan.hasNext()){
        	fscan.next();
        	count++;
        }
        
        fout.printf("total : %d", count);

        fscan.close();
        fis.close();
        fout.close();
        fos.close();
	}
}
