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

package common.statistic;

public class AverageStatistic extends AbstractCummulativeStatistic<Long> {

	private long sum;
	private long count;
	private long prevSec;

	public AverageStatistic(String outputFile, boolean autoFlush) {
		super(outputFile, autoFlush);
	}

	@Override
	protected void doAppend(Long v) {
		writePreviousAverages();
		sum += v;
		count++;
	}

	@Override
	public void enable() {
		this.sum = 0;
		this.count = 0;
		prevSec = currentTimeSeconds();
		super.enable();

	}

	public void disable() {
		writePreviousAverages();
		super.disable();
	}

	private void writePreviousAverages() {
		long thisSec = currentTimeSeconds();
		while (prevSec < thisSec) {
			long average = (count != 0 ? sum / count : -1);
			writeCommaSeparatedValues(prevSec, average);
			sum = 0;
			count = 0;
			prevSec++;
		}
	}
}