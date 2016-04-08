package com.lj.sqoop;

/**
 * 参考网址：
 * http://sqoop.apache.org/docs/1.99.6/ConnectorDevelopment.html
 * 通过官方文档可知，使用connector的时候，只有需要使用sqoop不支持的数据源时，才需要自写connector
 * connector宝库俩个方面：
 * 一个from：from包括Initializer, Partitioner, Extractor and Destroyer，主要为抽取数据
 * to包括：Initializer, Loader and Destroyer，主要为装载数据
 * @author Administrator
 *
 */
public class SqoopConnector {

}
