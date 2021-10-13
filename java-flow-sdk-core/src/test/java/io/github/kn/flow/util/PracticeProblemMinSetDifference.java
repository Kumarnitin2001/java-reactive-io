package io.github.kn.flow.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * Given a set of integers, the task is to divide it into two sets S1 and S2
 * such that the absolute difference between their sums is minimum. If there is
 * a set S with n elements, then if we assume Subset1 has m elements, Subset2
 * must have n-m elements and the value of abs(sum(Subset1) – sum(Subset2))
 * should be minimum
 * 
 * @author kumar
 *
 */
public class PracticeProblemMinSetDifference extends AbstractPracticeProblem {
	private final Map<String, Integer> memoizeMap = new HashMap<>();

	int closestDifference(final List<Integer> input, final int index, final int target) {
		Integer memorizedResult = memoizeMap.get(index + ":" + target);
		if (memorizedResult != null) {
			return memorizedResult;
		} else {
			int difference = target;
			if (index == 0) {
				difference = Math.abs(input.get(0) - target);
			} else {
				int diff1 = closestDifference(input, index - 1, Math.abs(target - input.get(index)));
				int diff2 = closestDifference(input, index - 1, Math.abs(target + input.get(index)));
				difference = Math.min(diff1, diff2);
			}
			memoizeMap.putIfAbsent(index + ":" + target, difference);
			return difference;
		}
	}

	public static void main(String[] args) {
		PracticeProblemMinSetDifference problem = new PracticeProblemMinSetDifference();

//		List<Integer> inputSet = Arrays.asList(1, 5, 11, 6);

		List<Integer> inputSet = new Random().ints().map(c -> Math.abs(c)).filter(c -> c < 99999).distinct().limit(45)
				.boxed().collect(Collectors.toList());

		long beginTime = System.currentTimeMillis();
		System.out.println(problem.closestDifference(inputSet, inputSet.size() - 1, 0));
		printTotalTime(beginTime);
//		System.out.println(problem.memoizeMap);
	}

}
