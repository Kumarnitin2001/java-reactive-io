package io.github.kn.flow.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Given a value N, if we want to make change for N cents, and we have infinite
 * supply of each of S = { S1, S2, .. , Sm} valued coins, how many ways can we
 * make the change? The order of coins doesn’t matter. For example, for N = 4
 * and S = {1,2,3}, there are four solutions: {1,1,1,1},{1,1,2},{2,2},{1,3}. So
 * output should be 4. For N = 10 and S = {2, 5, 3, 6}, there are five
 * solutions: {2,2,2,2,2}, {2,2,3,3}, {2,2,6}, {2,3,5} and {5,5}. So the output
 * should be 5.
 * 
 * @author kumar
 *
 */
public class PracticeProblemCoinChange extends AbstractPracticeProblem {

	private final Map<String, Long> memoizeMap = new HashMap<String, Long>();

	static long countWays(Integer[] S, int m, int n) {
		// Time complexity of this function: O(mn)
		// Space Complexity of this function: O(n)

		// table[i] will be storing the number of solutions
		// for value i. We need n+1 rows as the table is
		// constructed in bottom up manner using the base
		// case (n = 0)
		long[] table = new long[n + 1];

		// Initialize all table values as 0
		Arrays.fill(table, 0); // O(n)

		// Base case (If given value is 0)
		table[0] = 1;

		// Pick all coins one by one and update the table[]
		// values after the index greater than or equal to
		// the value of the picked coin
		for (int i = 0; i < m; i++)
			for (int j = S[i]; j <= n; j++)
				table[j] += table[j - S[i]];

		return table[n];
	}

	private long numChanges(final int targetValue, final List<Integer> coinValues, final int size) {

		Long memorizedNumChanges = memoizeMap.get(targetValue + ":" + size);

		if (memorizedNumChanges != null) {
			return memorizedNumChanges;
		} else {
			long numChanges = 0;
			if (size == 1 && targetValue % coinValues.get(size - 1) == 0) {
				numChanges = 1;
			} else if (size > 1 && targetValue >= 0) {
				for (int i = 0; targetValue >= i * coinValues.get(size - 1); i++) {
					numChanges += numChanges(targetValue - (i * coinValues.get(size - 1)), coinValues, size - 1);
				}

			}
			memoizeMap.putIfAbsent(targetValue + ":" + size, numChanges);
			return numChanges;
		}
	}

	public static void main(String[] args) {

		List<Integer> coinValues = Arrays.asList(2, 5, 3, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17);

		PracticeProblemCoinChange problem = new PracticeProblemCoinChange();

		long beginTime = System.currentTimeMillis();
		System.out.println(problem.numChanges(1000, coinValues, coinValues.size()));
//		System.out.println(countWays(coinValues.toArray(new Integer[0]), coinValues.size(), 1000));
		printTotalTime(beginTime);

	}

}
