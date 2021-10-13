package io.github.kn.flow.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 
 * @author kumar
 * 
 * 
 */
public class PracticeProblemAlphaCoding  extends AbstractPracticeProblem {

	private Map<String, Object> memoizeMap = new HashMap<String, Object>();

	private final List<String> alphabets = Arrays.asList("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L",
			"M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z");

	private List<String> representations(String number) {

		if (memoizeMap.containsKey(number)) {
			return (List<String>) memoizeMap.get(number);
		}
		final List<String> result = new ArrayList<>();
		if (number.isBlank()) {
			return Collections.emptyList();
		}

		if (number.length() == 1) {
			return Collections.singletonList(getAlphabet(number));
		}

		if (number.length() == 2) {
			return lessThanTwentySeven(number)
					? Arrays.asList(getAlphabet(number.substring(0, 1)) + getAlphabet((number.substring(1))),
							getAlphabet(number))
					: Arrays.asList(getAlphabet(number.substring(0, 1)) + getAlphabet((number.substring(1))));
		}

		representations(number.substring(1)).forEach(s -> result.add(getAlphabet(number.substring(0, 1)) + s));

		if (lessThanTwentySeven(number.substring(0, 2))) {
			representations(number.substring(2)).forEach(s -> result.add((getAlphabet(number.substring(0, 2)) + s)));
		}
		memoizeMap.putIfAbsent(number, result);
		return result;
	}

	private String getAlphabet(String number) {
		return alphabets.get(Integer.valueOf(number) - 1);
	}

	private long representationsCount(String number) {

//		if (memoizeMap.containsKey(number)) {
//			return (long) memoizeMap.get(number);
//		}

		long returnCount = 0;

		if (number.isBlank()) {
			returnCount = 0;
		} else if (number.length() == 1) {
			returnCount = 1;
		} else if (number.length() == 2) {
			returnCount = lessThanTwentySeven(number) ? 2 : 1;
		} else {

			returnCount = representationsCount(number.substring(1))
					+ (lessThanTwentySeven(number.substring(0, 2)) ? (representationsCount(number.substring(2))) : 0);
		}
//		memoizeMap.putIfAbsent(number, returnCount);

		return returnCount;
	}

	private boolean lessThanTwentySeven(String number) {
		return Integer.valueOf(number) < 27;
	}

	public static void main(String[] args) {
//		System.out.println(new PracticeProblems().representations("332"));
//		System.out.println(new PracticeProblemAlphaCoding()
//				.representations("213233256544534546213123342423423123123411213123123121312").size());
		long beginTime = System.currentTimeMillis();
		System.out.println(new PracticeProblemAlphaCoding().representationsCount(
				"213233256544534546213123341234512312212313123231123423123423423123123411213123123121312"));
		printTotalTime(beginTime);
	}

}
