/*  Copyright (C) 2017  Vincenzo Gulisano
 * 
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *  
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *  
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *  
 *  Contact: Vincenzo Gulisano info@vincenzogulisano.com
 *
 */

package common.util;

import java.util.Random;

public class BackOff {

	private final Random rand = new Random();
	final int min, max, retries;
	int currentRetries, currentLimit;

	public static BackOff newIncreasing(int min, int max, int retries) {
		return new BackOff(min, max, min, retries, retries);
	}

	public static BackOff newDecreasing(int min, int max, int retries) {
		return new BackOff(min, max, max, retries, 0);
	}

	protected BackOff(int min, int max, int currentLimit, int retries, int currentRetries) {
		this.min = min;
		this.max = max;
		this.retries = retries;
		this.currentLimit = currentLimit;
		this.currentRetries = currentRetries;
	}

	public void backoff() {

		int delay = rand.nextInt(currentLimit);
		currentRetries--;
		if (currentRetries == 0) {
			currentLimit = (2 * currentLimit < max) ? 2 * currentLimit : max;
			currentRetries = retries;
		}

		Util.sleep(delay);
	}

	public void relax() {
		if (currentRetries < retries) {
			currentRetries++;
			if (currentRetries == retries) {
				currentLimit = (currentLimit / 2 >= min) ? currentLimit / 2 : min;
				currentRetries = 0;
			}
		}
	}

	@Override
	public String toString() {
		return "BackOff [min=" + min + ", max=" + max + ", retries=" + retries + ", currentLimit=" + currentLimit
				+ ", currentRetries=" + currentRetries + "]";
	}

}