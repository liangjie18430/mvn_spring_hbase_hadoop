package com.lj.tools;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.conf.Configuration;

public class ResourcesConfig {
	/*public  static void main(String[] args){
		//获取系统路径
		String dir1 = System.getProperty("user.dir");
		
		//String dir2 = ResourcesConfig.class.getClassLoader().getResource("").getPath();
		//ResourcesConfig.class.getClassLoader().getResource("").getPath();
		
		
		//File.separator;

		//String dir3 = ResourcesConfig.class.getResource(".." + File.separator + ".." + File.separator + "").getPath(); // 这种方法相当于使用相对MyClass的运行时路径
		//String dir4 = ResourcesConfig.class.getResource(File.separator + "hbase"+File.separator+"hbase-site.xml").getPath(); // 这种方法相当于使用绝对运行时路径
		
		
		//拼凑出资源路径
		dir1=dir1+File.separator+"target"+File.separator+"classes"+File.separator+"hbase"+File.separator+"hbase.xml";
		//Path path = Paths.get(dir1);
		//System.out.println(path);
		//System.out.println(dir2);
		//System.out.println(dir3);
		//System.out.println(dir4);
	}*/
	
	public static Configuration getHbaseConfig(String fileName){
		String quorum = "172.16.135.160,172.16.135.161";
		//String dir = System.getProperty("user.dir");
		//dir=dir+File.separator+"target"+File.separator+"classes"+File.separator+"hbase"+File.separator+fileName;
		//Path path = Paths.get(dir);
		//Path path = new Path(dir);
		Configuration config = HBaseConfiguration.create();
		//config.set("hbase.zookeeper.quorum", "B507Server1,b507Server2");
		/** 与hbase/conf/hbase-site.xml中hbase.master配置的值相同 */
		config.set("hbase.master", "192.16.135.160:60000");
		/** 与hbase/conf/hbase-site.xml中hbase.zookeeper.quorum配置的值相同 */
		//当配置中有个zookeeper挂掉后，可以将挂掉的ip注释掉
		//config.set("hbase.zookeeper.quorum", "B507Server1,b507Server2,b507Server3");
		config.set("hbase.zookeeper.quorum", quorum);
		/** 与hbase/conf/hbase-site.xml中hbase.zookeeper.property.clientPort配置的值相同 */
		config.set("hbase.zookeeper.property.clientPort", "2181");
		//config.addResource(path);
		return config;
		
	}

	public static Configuration getHbaseConfig(){
		String quorum = "172.16.135.160,172.16.135.161";
		//String dir = System.getProperty("user.dir");
		//dir=dir+File.separator+"target"+File.separator+"classes"+File.separator+"hbase"+File.separator+fileName;
		//Path path = Paths.get(dir);
		//Path path = new Path(dir);
		Configuration config = HBaseConfiguration.create();
		//config.set("hbase.zookeeper.quorum", "B507Server1,b507Server2");
		/** 与hbase/conf/hbase-site.xml中hbase.master配置的值相同 */
		config.set("hbase.master", "192.16.135.160:60000");
		/** 与hbase/conf/hbase-site.xml中hbase.zookeeper.quorum配置的值相同 */
		//当配置中有个zookeeper挂掉后，可以将挂掉的ip注释掉
		//config.set("hbase.zookeeper.quorum", "B507Server1,b507Server2,b507Server3");
		config.set("hbase.zookeeper.quorum", quorum);
		/** 与hbase/conf/hbase-site.xml中hbase.zookeeper.property.clientPort配置的值相同 */
		config.set("hbase.zookeeper.property.clientPort", "2181");
		//config.addResource(path);
		return config;
		
	}
	/*public static Configuration getHadoopConfig(String fileName){
		String dir = System.getProperty("user.dir");
		dir=dir+File.separator+"target"+File.separator+"classes"+File.separator+"hbase"+File.separator+fileName;
		//Path path = Paths.get(dir);
		Path path = new Path(dir);
		Configuration config = HBaseConfiguration.create();
		config.addResource(path);
		return config;
		
	}*/
}
