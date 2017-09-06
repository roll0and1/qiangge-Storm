package com.qiangge.topology;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest
		extends TestCase {
	/**
	 * Create the test case
	 *
	 * @param testName name of the test case
	 */
	public AppTest(String testName) {
		super(testName);
	}

	/**
	 * @return the suite of tests being tested
	 */
	public static Test suite() {
		return new TestSuite(AppTest.class);
	}

	/**
	 * Rigourous Test :-)
	 */
	public void testApp() {
		assertTrue(true);
	}


	public static void main(String[] args) {
		int target = 6;
		int[] nums = {3, 2, 4, 1, 2};

		int[] result = new int[2];
		int left = 0;
		int right = nums.length - 1;
		outterloop:
		for (int i = 0; i < nums.length; i++) {
			int leftNum = nums[i];
			for (int j = right; j > i; j--) {
				int rightNum = nums[j];
				if (leftNum + rightNum == target) {
					result[0] = i;
					result[1] = j;
					break outterloop;
				}
			}
		}
		System.out.println(result[0] + "," + result[1]);
	}


}
