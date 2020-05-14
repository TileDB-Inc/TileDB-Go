package tiledb

import "time"

const secondsInCommonYear = 31536000
const secondsInLeapYear = 31622400
const secondsInDay = 24 * 60 * 60
const secondsInHour = 60 * 60
const secondsInMin = 60
const epochYear = 1970

func isLeapYear(year int) bool {
	leapFlag := false
	if year%4 == 0 {
		if year%100 == 0 {
			if year%400 == 0 {
				leapFlag = true
			} else {
				leapFlag = false
			}
		} else {
			leapFlag = true
		}
	} else {
		leapFlag = false
	}
	return leapFlag
}

func secondsFromEpochYears(numOfYears int64) int64 {
	var numOfSeconds int64 = 0

	if numOfYears > 0 {
		for y := int64(epochYear); y < epochYear+numOfYears; y++ {
			if isLeapYear(int(y)) {
				numOfSeconds += secondsInLeapYear
			} else {
				numOfSeconds += secondsInCommonYear
			}
		}
	} else {
		for y := int64(epochYear) - 1; y >= epochYear+numOfYears; y-- {
			if isLeapYear(int(y)) {
				numOfSeconds -= secondsInLeapYear
			} else {
				numOfSeconds -= secondsInCommonYear
			}
		}
	}

	return numOfSeconds
}

// abs returns the absolute value of x
func abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

func secondsFromEpochMonths(numOfMonths int64) int64 {
	numOfYears := numOfMonths / 12
	numOfSeconds := secondsFromEpochYears(numOfYears)
	numOfMonthsInLastYear := abs(numOfMonths) % 12

	if numOfMonths > 0 {
		for m := int64(1); m <= numOfMonthsInLastYear; m++ {
			switch m {
			case 1:
				numOfSeconds += 31 * secondsInDay
			case 2:
				currentYear := epochYear + numOfYears + 1
				if isLeapYear(int(currentYear)) {
					numOfSeconds += 29 * secondsInDay
				} else {
					numOfSeconds += 28 * secondsInDay
				}
			case 3:
				numOfSeconds += 31 * secondsInDay
			case 4:
				numOfSeconds += 30 * secondsInDay
			case 5:
				numOfSeconds += 31 * secondsInDay
			case 6:
				numOfSeconds += 30 * secondsInDay
			case 7:
				numOfSeconds += 31 * secondsInDay
			case 8:
				numOfSeconds += 31 * secondsInDay
			case 9:
				numOfSeconds += 30 * secondsInDay
			case 10:
				numOfSeconds += 31 * secondsInDay
			case 11:
				numOfSeconds += 30 * secondsInDay
			case 12:
				numOfSeconds += 31 * secondsInDay
			}
		}
	} else {
		for m := int64(12); m > 12-numOfMonthsInLastYear; m-- {
			switch m {
			case 1:
				numOfSeconds -= 31 * secondsInDay
			case 2:
				currentYear := epochYear - numOfYears - 1
				if isLeapYear(int(currentYear)) {
					numOfSeconds -= 29 * secondsInDay
				} else {
					numOfSeconds -= 28 * secondsInDay
				}
			case 3:
				numOfSeconds -= 31 * secondsInDay
			case 4:
				numOfSeconds -= 30 * secondsInDay
			case 5:
				numOfSeconds -= 31 * secondsInDay
			case 6:
				numOfSeconds -= 30 * secondsInDay
			case 7:
				numOfSeconds -= 31 * secondsInDay
			case 8:
				numOfSeconds -= 31 * secondsInDay
			case 9:
				numOfSeconds -= 30 * secondsInDay
			case 10:
				numOfSeconds -= 31 * secondsInDay
			case 11:
				numOfSeconds -= 30 * secondsInDay
			case 12:
				numOfSeconds -= 31 * secondsInDay
			}
		}
	}

	return numOfSeconds
}

// GetTimeFromTimestamp returns a time.Time object for a time related TileDB datatype
// Datetimes in TileDB are deltas from unix epoch with a resolution of the specified time
func GetTimeFromTimestamp(datatype Datatype, timestamp int64) time.Time {
	var then time.Time
	switch datatype {
	case TILEDB_DATETIME_YEAR:
		numOfSeconds := secondsFromEpochYears(timestamp)
		then = time.Unix(int64(numOfSeconds), 0)
	case TILEDB_DATETIME_MONTH:
		numOfSeconds := secondsFromEpochMonths(timestamp)
		then = time.Unix(int64(numOfSeconds), 0)
	case TILEDB_DATETIME_WEEK:
		numOfSeconds := 7 * timestamp * secondsInDay
		then = time.Unix(int64(numOfSeconds), 0)
	case TILEDB_DATETIME_DAY:
		numOfSeconds := timestamp * secondsInDay
		then = time.Unix(int64(numOfSeconds), 0)
	case TILEDB_DATETIME_HR:
		numOfSeconds := timestamp * secondsInHour
		then = time.Unix(int64(numOfSeconds), 0)
	case TILEDB_DATETIME_MIN:
		numOfSeconds := timestamp * secondsInMin
		then = time.Unix(int64(numOfSeconds), 0)
	case TILEDB_DATETIME_SEC:
		then = time.Unix(timestamp, 0)
	case TILEDB_DATETIME_MS:
		then = time.Unix(0, int64(timestamp*1000*1000))
	case TILEDB_DATETIME_US:
		then = time.Unix(0, int64(timestamp*1000))
	case TILEDB_DATETIME_NS:
		then = time.Unix(0, timestamp)
	case TILEDB_DATETIME_PS:
		then = time.Unix(0, int64(timestamp/1000))
	case TILEDB_DATETIME_FS:
		then = time.Unix(0, int64(timestamp/(1000*1000)))
	case TILEDB_DATETIME_AS:
		then = time.Unix(0, int64(timestamp/(1000*1000)))
	}

	return then.UTC()
}
