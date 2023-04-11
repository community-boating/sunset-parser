import * as fs from 'fs';
import { parse } from 'csv';
import * as Moment from 'moment';
import moment from 'moment';

type Row = {
	day: string,
	twilightStart: string,
	sunrise: string,
	sunset: string,
	twilightEnd: string,
	dayLength: string,
	solarNoon: string,
	nauticalTwilightStart: string,
	nauticalTwilightEnd: string,
	astronomicalTwilightStart: string,
	astronomicalTwilightEnd: string
}

function readFile(year: string) {
	return new Promise<Row[]>((resolve, reject) => {
		const parser = parse({delimiter: ','}, function(err, data){
			resolve(data.map((r: any) => ({
				day: r[0],
				twilightStart: r[1],
				sunrise: r[2],
				sunset: r[3],
				twilightEnd: r[4],
				dayLength: r[5],
				solarNoon: r[6],
				nauticalTwilightStart: r[7],
				nauticalTwilightEnd: r[8],
				astronomicalTwilightStart: r[9],
				astronomicalTwilightEnd: r[10]
			})))
		});
		
		fs.createReadStream(`data/sunsets-${year}.csv`).pipe(parser);
	})
}

const makeMoment = (prefix: string, time: string, seconds: boolean) => {
	const format = (
		seconds
		? "MMM D YYYY h:mm:ss a"
		: "MMM D YYYY h:mm a"
	);

	return moment(`${prefix} ${time}`, format)
}

const formatMomentForSql = (m: Moment.Moment) => `TO_DATE('${m.format("MM-DD-YYYY hh:mm:ssA")}', 'MM-DD-YYYY HH:MI:SSPM')`

function parseDayLength(l: string) {
	const regex = /^(\d{2}):(\d{2}):(\d{2})$/
	const [match, hours, minutes, seconds] = (regex.exec(l) || [])
	return (Number(hours) * 60 * 60) + (Number(minutes) * 60) + Number(seconds)
}

const parseRowToSql = (year: string) => (row: Row) => {
	const dayRegex = /^\w+, ([\w ]+)$/
	const dayString = (dayRegex.exec(row.day) || [])[1];
	const d = (time: string, seconds: boolean) => formatMomentForSql(makeMoment(`${dayString} ${year}`, time, seconds))

	const values = [{
		col: "FOR_DATE",
		value: d("12:00 a", false)
	}, {
		col: "TWILIGHT_START",
		value: d(row.twilightStart, true)
	}, {
		col: "SUNRISE",
		value: d(row.sunrise, true)
	}, {
		col: "SUNSET",
		value: d(row.sunset, true)
	}, {
		col: "TWILIGHT_END",
		value: d(row.twilightEnd, true)
	}, {
		col: "DAY_LENGTH_SECONDS",
		value: parseDayLength(row.dayLength)
	}, {
		col: "SONAR_NOON",
		value: d(row.solarNoon, true)
	}, {
		col: "NAUTICAL_TWILIGHT_START",
		value: d(row.nauticalTwilightStart, false)
	}, {
		col: "NAUTICAL_TWILIGHT_END",
		value: d(row.nauticalTwilightEnd, false)
	}, {
		col: "ASTRONOMICAL_TWILIGHT_START",
		value: d(row.astronomicalTwilightStart, false)
	}, {
		col: "ASTRONOMICAL_TWILIGHT_END",
		value: d(row.astronomicalTwilightEnd, false)
	}];

	return `INSERT INTO SUNSET_TIMES (${values.map(v => v.col).join(",")}) VALUES (${values.map(v => v.value).join(",")});`
}

function main() {
	const year = process.argv[2]
	console.log(year)
	readFile(year).then(res => {
		res.map(parseRowToSql(year)).forEach(sql => console.log(sql))
	})
}

main();