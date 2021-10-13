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
public class PracticeProblemLCS extends AbstractPracticeProblem {
	private final Map<String, Integer> memoizeMap = new HashMap<>();

	/* Returns length of LCS for X[0..m-1], Y[0..n-1] */
	private int lcs(char[] X, char[] Y, int m, int n) {
		int L[][] = new int[m + 1][n + 1];

		/*
		 * Following steps build L[m+1][n+1] in bottom up fashion. Note that L[i][j]
		 * contains length of LCS of X[0..i-1] and Y[0..j-1]
		 */
		for (int i = 0; i <= m; i++) {
			for (int j = 0; j <= n; j++) {
				if (i == 0 || j == 0)
					L[i][j] = 0;
				else if (X[i - 1] == Y[j - 1])
					L[i][j] = L[i - 1][j - 1] + 1;
				else
					L[i][j] = Math.max(L[i - 1][j], L[i][j - 1]);
			}
		}
		return L[m][n];
	}

	int lcsLength(final String a, final String b) {

		Integer memorizedLcs = memoizeMap.get(a.length()+":"+b.length());
		if(memorizedLcs !=null) {
			return memorizedLcs;
		}
		
		int lengthToReturn = 0;
		if (a.length() == 1 && b.length() > 0) {
			lengthToReturn = b.indexOf(a) == -1 ? 0 : 1;
		} else if (b.length() == 1 && a.length() > 0) {
			lengthToReturn = a.indexOf(b) == -1 ? 0 : 1;
		} else {
			if (a.startsWith(b.substring(0, 1))) {
				lengthToReturn = 1 + lcsLength(a.substring(1), b.substring(1));
			} else {
				lengthToReturn = Math.max(lcsLength(a.substring(0), b.substring(1)),
						lcsLength(a.substring(1), b.substring(0)));
			}
		}
//		memoizeMap.putIfAbsent(a.length()+":"+b.length(), lengthToReturn);
		return lengthToReturn;
	}

	public static void main(String[] args) {
		PracticeProblemLCS problem = new PracticeProblemLCS();

		String a = "LKJKGFKJGHKFJHHWEYEGREGSDJHGFDSHGFUEWYGREYGJHFSJDYGFYGRYFGSJHDFGJSHGFJDSHGFUY";
		String b = "FGSDHFETYWFETDFJGFFYTEFYETFDHGSCHGZGSDFWRFDWDFGSHFDGHSFDTEFTYFDZGSCSGFDSARDTSRD";

		long beginTime = System.currentTimeMillis();
		System.out.println(problem.lcsLength(a, b));
		System.out.println(problem.lcs(a.toCharArray(), b.toCharArray(), a.length(), b.length()));
		printTotalTime(beginTime);
//		System.out.println(problem.memoizeMap);
	}

}
