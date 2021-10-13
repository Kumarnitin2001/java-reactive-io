package io.github.kn.flow.util;

import java.util.Arrays;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * Given an array and a number k where k is smaller than the size of the array,
 * we need to find the k’th smallest element in the given array. It is given
 * that all array elements are distinct.
 * 
 * @author kumar
 *
 */
public class PracticeProblemQuickSelect extends AbstractPracticeProblem {

	/**
	 * {1, 4, 3, 6, 2} {1, 4, 3, 6, 2} {1, 2, 3, 4, 6}
	 * 
	 * @param arr
	 * @param index
	 * @return
	 */
	private int partition(Integer[] arr, int index) {
		int selected = arr[index];
		int i = 0, j = arr.length-1;
		for (; i < j;) {
			if (arr[i] < selected) {
				i++;
				continue;
			}
			if (arr[j] > selected) {
				j--;
				continue;
			}
			swap(arr, i, j);
		}
		return i;
	}

	private void swap(Integer[] arr, int i, int j) {
		if (i != j) {
			int temp = arr[i];
			arr[i] = arr[j];
			arr[j] = temp;
		}
	}

	private int quickSelect(Integer[] arr, int k) {
		int pivot = partition(arr, 0);
		int kSmallest = arr[pivot];
		if (k < pivot) {
			kSmallest = quickSelect(Arrays.copyOfRange(arr, 0, pivot), k);
		} else if (k > pivot) {
			kSmallest = quickSelect(Arrays.copyOfRange(arr, pivot+1, arr.length), k-(pivot+1));
		}
		return kSmallest;
	}

	public static void main(String[] args) {
		PracticeProblemQuickSelect problem = new PracticeProblemQuickSelect();
		Integer[] intArray = new Random().ints().map(c -> Math.abs(c)).filter(c -> c < 1000).distinct().limit(10)
				.boxed().collect(Collectors.toList()).toArray(new Integer[0]);

		System.out.println(Arrays.toString(intArray));
		long beginTime = System.currentTimeMillis();
		System.out.println(problem.quickSelect(intArray, 8));
		printTotalTime(beginTime);
//		System.out.println(problem.memoizeMap);
	}

}
