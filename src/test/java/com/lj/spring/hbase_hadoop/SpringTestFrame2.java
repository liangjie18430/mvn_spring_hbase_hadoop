package com.lj.spring.hbase_hadoop;

import java.util.UUID;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;

/**
 * @Transactional is use to make the method has transaction
 * @author root
 *
 */



//@Transactional
public class SpringTestFrame2 {
	
	HbaseTemplate hbaseTest;
	
	
	@Test
    public void test() {
    	ClassPathXmlApplicationContext config = new ClassPathXmlApplicationContext("classpath:spring/applicationContext.xml");
    	hbaseTest = (HbaseTemplate) config.getBean("htemplate");
		hbaseTest.find("test2", "article", new RowMapper<String>() {

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
