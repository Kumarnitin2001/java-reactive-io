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
public class PracticeProblemMakeBeautiful extends AbstractPracticeProblem {

	private int numChanges(final String inpStr) {

		int opCount1 = 0;
		int opCount2 = 0;
		for (int i = 0; i < inpStr.length(); i++) {

			if (i % 2 == 0) {
				if (inpStr.charAt(i) == '0') {
					opCount1++;
				} else {
					opCount2++;
				}

			} else {
				if (inpStr.charAt(i) == '1') {
					opCount1++;
				} else {
					opCount2++;
				}
			}

		}
		return Math.min(opCount2, opCount1);
	}

	public static void main(String[] args) {
		System.out.println(new PracticeProblemMakeBeautiful().numChanges("1001"));
	}

}
