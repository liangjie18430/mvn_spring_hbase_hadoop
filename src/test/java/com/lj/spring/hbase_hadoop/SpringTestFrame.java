package com.lj.spring.hbase_hadoop;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;

/**
 * @Transactional is use to make the method has transaction
 * 
 * Test is okay
 * @author root
 *
 */


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"classpath:spring/applicationContext.xml"})
//@Transactional
public class SpringTestFrame {
	//@Autowired :Default use class type to match
	@Autowired
	HbaseTemplate hbaseTest;
	
	
	@Test
	@Transactional
    public void test() {
		//search the default namespace data;
		System.out.println("hbase.zookeeper.quorum:"+hbaseTest.getConfiguration().get("hbase.zookeeper.quorum"));
		System.out.println("hbase.master:"+hbaseTest.getConfiguration().get("hbase.master"));
		System.out.println("hbase.zookeeper.property.clientPort:"+hbaseTest.getConfiguration().get("hbase.zookeeper.property.clientPort"));
		//hbaseTest.getConfiguration().set("hbase.master","172.16.135.160:60000");
		//System.out.println("hbase.master2:"+hbaseTest.getConfiguration().get("hbase.master"));
		hbaseTest.find("blog", "article", new RowMapper<String>() {

			@Override
			public String mapRow(Result result, int rowNum) throws Exception {
				// TODO Auto-generated method stub
				
				//cell is use rowkey,family,qualifier
				for(Cell cell:result.rawCells()){
					String value = new String(CellUtil.cloneValue(cell));
					System.out.println("Get the value:"+value);
					
				}
				return null;
			}
		});
       /* ApplicationContext ac = new ClassPathXmlApplicationContext(new String[] { "classpath:spring/applicationContext.xml" });  
        TestService userService = (TestServiceImpl) ac.getBean("testService"); */
        
      //  st.setId(UUID.randomUUID().toString());
        /*st.setName("test2");
        st.setAge(20);*/
       
    }  

}
