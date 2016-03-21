package com.lj.tools;

import java.util.List;

public class PrintTool {
	public static void print(Object[] array) {
		if (array == null) {
			System.out.println("传入的数组为空");
		} else {
			if (array.length == 0) {
				System.out.println("不存在需要输出的数据");
			} else {
				for (Object o : array) {
					System.out.println(o.toString());
				}
			}
		}
	}

	public static void print(List<?> array) {
		if (array == null) {
			System.out.println("传入的队列为空");
		} else {
			if (array.size() == 0) {
				System.out.println("不存在需要输出的数据");
			} else {
				for (Object o : array) {
					System.out.println(o.toString());
				}
			}
		}
	}
	
	
	public static void print(Object obj) {
		if (obj == null) {
			System.out.println("传入的对象为空");
		} else {
			System.out.println(obj.toString());
		}
	}

}
