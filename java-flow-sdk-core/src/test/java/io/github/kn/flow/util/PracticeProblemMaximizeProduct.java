package io.github.kn.flow.util;

import java.util.HashMap;
import java.util.Map;

/**
 * Given a rope of length n meters, cut the rope in different parts of integer
 * lengths in a way that maximizes product of lengths of all parts. You must
 * make at least one cut. Assume that the length of rope is more than 2 meters.
 * 
 * @author kumar
 *
 */
public class PracticeProblemMaximizeProduct extends AbstractPracticeProblem {
	private final Map<Long, Long> memoizeMap = new HashMap<>();

    static int maxProd(int n)
    {
        // Base cases
        if (n == 0 || n == 1) return 0;
 
        // Make a cut at different places
        // and take the maximum of all
        int max_val = 0;
        for (int i = 1; i < n; i++)
        max_val = Math.max(max_val,
                  Math.max(i * (n - i),
                   maxProd(n - i) * i));
 
        // Return the maximum of all values
        return max_val;
    }  
	
	long maxProduct(final long residualLength) {
		Long memorizedResult = memoizeMap.get(residualLength);
		if (memorizedResult != null) {
			return memorizedResult;
		} else {
			long maxProduct = residualLength;
			for (long i = 1; i < residualLength; i++) {
				long product = maxProduct(i) * maxProduct(residualLength - i);
				if (product > maxProduct) {
					maxProduct = product;
				}
			}
			memoizeMap.putIfAbsent(residualLength, maxProduct);
			return maxProduct;
		}
	}

	public static void main(String[] args) {
		PracticeProblemMaximizeProduct problem = new PracticeProblemMaximizeProduct();


		long beginTime = System.currentTimeMillis();
		System.out.println(problem.maxProduct(100));
		//System.out.println(maxProd(3));
		printTotalTime(beginTime);
//		System.out.println(problem.memoizeMap);
	}

}
