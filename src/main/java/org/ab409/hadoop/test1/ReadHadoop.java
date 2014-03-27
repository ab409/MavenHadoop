package org.ab409.hadoop.test1;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.eclipse.jdt.core.dom.ThisExpression;

public class ReadHadoop
{
	private static FileSystem getFileSystem() throws URISyntaxException, IOException
	{
		Configuration conf = new Configuration();
		
		URI uri = new URI("hdfs://hadoop:9000");
		
		final FileSystem fileSystem = FileSystem.get(uri , conf);
		
		return fileSystem;
	}
	
	private static void readFile() throws Exception
	{
		FileSystem fileSystem = getFileSystem();
		
		FSDataInputStream openStream = fileSystem.open(new Path("hdfs://hadoop:9000/test2.txt"));
		
		IOUtils.copyBytes(openStream, System.out, 1024, false);
		
		IOUtils.closeStream(openStream);
	}
	
	private static void writeFile() throws URISyntaxException, IOException
	{
		FileSystem fileSystem = getFileSystem();
		
		FSDataOutputStream outputStream = fileSystem.create(new Path("hdfs://hadoop:9000/test2.txt"));
		
		FileInputStream inputStream = new FileInputStream("Pom.xml");
		
		IOUtils.copyBytes(inputStream, outputStream, 1024,false);
		
		IOUtils.closeStream(outputStream);
		
		inputStream.close();
	}
	
	private static void mkdir() throws URISyntaxException, IOException
	{
		FileSystem fileSystem = getFileSystem();
		
		fileSystem.mkdirs(new Path("hdfs://hadoop:9000/dir1"));
	}
	
	@SuppressWarnings("deprecation")
	private static void rmdir() throws URISyntaxException, IOException
	{
		FileSystem fileSystem = getFileSystem();
		
		fileSystem.delete(new Path("hdfs://hadoop:9000/dir1"), false);
	}
	
	private static void list() throws IOException, URISyntaxException
	{
		FileSystem fileSystem = getFileSystem();
		
		FileStatus[] fileStatusArray = fileSystem.listStatus(new Path("hdfs://hadoop:9000/"));
		
		for(FileStatus fileStatus : fileStatusArray)
		{
			String isDir = fileStatus.isDir()? "目录":"文件";
			
			String fileName = fileStatus.getPath().toString();
			
			System.out.println(isDir + " " + fileName);
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		readFile();
		
//		mkdir();
		
//		rmdir();
		
//		list();
		
//		writeFile();
	}

}
