package io.github.kn.flow.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Given a rod of length n inches and an array of prices that includes prices of
 * all pieces of size smaller than n. Determine the maximum value obtainable by
 * cutting up the rod and selling the pieces. For example, if the length of the
 * rod is 8 and the values of different pieces are given as the following, then
 * the maximum obtainable value is 22 (by cutting in two pieces of lengths 2 and
 * 6)
 * 
 * @author kumar
 *
 */
public class PracticeProblemMaximizeProfitOnRodCutting extends AbstractPracticeProblem {

	private final Map<Integer, Integer> memoizeMap = new HashMap<Integer, Integer>();

	private int maxProfit(final int totalLength, final List<Integer> lengthPricing) {

		Integer memorizedPrice = memoizeMap.get(totalLength);
		if (memorizedPrice != null) {
			return memorizedPrice;
		} else {

			int maxProfit = lengthPricing.get(totalLength - 1);
			for (int i = 1; i <= totalLength / 2; i++) {
				int profit = maxProfit(totalLength - i, lengthPricing) + maxProfit(i, lengthPricing);

				if (profit > maxProfit) {
					maxProfit = profit;
				}
			}
			memoizeMap.putIfAbsent(totalLength, maxProfit);
			return maxProfit;
		}
	}

	static int cutRod(Integer price[], int n) {
		int val[] = new int[n + 1];
		val[0] = 0;

		// Build the table val[] in bottom up manner and return
		// the last entry from the table
		for (int i = 1; i <= n; i++) {
			int max_val = Integer.MIN_VALUE;
			for (int j = 0; j < i; j++)
				max_val = Math.max(max_val, price[j] + val[i - j - 1]);
			val[i] = max_val;
		}

		return val[n];
	}

	public static void main(String[] args) {

		List<Integer> lengthPricing = Arrays.asList(3, 5, 8, 9, 10, 17, 17, 20, 21, 22, 34, 35, 40, 41, 42, 43, 44, 48,
				49, 50, 51, 60, 61, 65, 66, 67, 69, 72, 76, 77, 78);

		PracticeProblemMaximizeProfitOnRodCutting problem = new PracticeProblemMaximizeProfitOnRodCutting();

		long beginTime = System.currentTimeMillis();
		System.out.println(problem.maxProfit(lengthPricing.size(), lengthPricing));
//		System.out.println(cutRod(lengthPricing.toArray(new Integer[0]), lengthPricing.size()));
		printTotalTime(beginTime);

	}

}
