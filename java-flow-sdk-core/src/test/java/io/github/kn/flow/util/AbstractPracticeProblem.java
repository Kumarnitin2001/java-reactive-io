package io.github.kn.flow.util;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * 
 * @author kumar
 *
 */
abstract class AbstractPracticeProblem {

	static void printTotalTime(long beginTime) {
		System.out.println(TimeUnit.SECONDS.convert(Duration.ofMillis(System.currentTimeMillis() - beginTime)));
	}

}
