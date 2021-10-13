package io.github.kn.flow.util;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 * @author kumar
 *
 */
public class PracticeProblemMinJumps extends AbstractPracticeProblem {

	private final Map<Integer, Integer> memoizeMap = new HashMap<>();

	int minJumps(int[] arr, int position) {
		Integer memorizedResult = memoizeMap.get(position);
		if (memorizedResult != null) {
			return memorizedResult;
		}

		if (arr.length == 0 || arr.length - 1 <= position) {
			return 0;
		} else {
			int returnJumps = -1;
			int element = arr[position];

			int minJumps = 1000000;
			for (int i = 1; i <= element; i++) {
				int jumps = minJumps(arr, position + i);
				if (jumps != -1) {
					minJumps = Math.min(minJumps, jumps);
				}
			}

			if (minJumps != 1000000) {
				returnJumps = 1 + minJumps;
			}
			memoizeMap.putIfAbsent(position, returnJumps);
			return returnJumps;
		}

	}

	public static void main(String[] args) {
		PracticeProblemMinJumps problem = new PracticeProblemMinJumps();

		long beginTime = System.currentTimeMillis();
		System.out.println(problem.minJumps(new int[] { 2, 3, 1, 1, 2, 4, 2, 0, 1, 1, 1, 4, 3, 1, 2, 1, 2, 3 }, 0));
		printTotalTime(beginTime);
//		System.out.println(problem.memoizeMap);
	}

}
