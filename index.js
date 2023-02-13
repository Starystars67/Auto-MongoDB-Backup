require('dotenv').config()
const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const spawn = require('child_process').spawn;
const notifier = require('node-notifier')
const path = require('path')
const fs = require('fs')
const moment = require('moment')

if (isMainThread) {
	notifier.notify({
	  title: 'MongoDB Automated Backup',
	  message: 'Backup Started!',
	  icon: path.join(__dirname, 'icon.png')
	});

	

	var dbs = process.env.DB.split(',')
	const threads = new Set();;
	
	for (var i = 0; i < dbs.length; i++) {
		var db = dbs[i]
		threads.add(new Worker(__filename, { workerData: { db: db }}));
	}
	
	for (let worker of threads) {
		worker.on('error', (err) => { throw err; });
		worker.on('exit', () => {
		  threads.delete(worker);
		  console.log(`Thread exiting, ${threads.size} running...`);
		  if (threads.size === 0) {
			console.log(`Backup Process Complete!`);
			process.exit()
		  }
		})
		worker.on('message', (msg) => {
		  
		});
	}

	fs.readdir(process.env.SAVELOCATION, (err, files) => {
	  var today = moment()
	  var logPeriod = process.env.LOGRETENTION.slice(-1)
	  var dataPeriod = process.env.BACKUPRETENTION.slice(-1)
	  var logInterval = process.env.LOGRETENTION.slice(0, (process.env.LOGRETENTION.length-1))
	  var dataInterval = process.env.BACKUPRETENTION.slice(0, (process.env.BACKUPRETENTION.length-1))
	  switch (logPeriod) {
		case 'h':
		  logPeriod = 'hours'
		  break;
		case 'd':
		  logPeriod = 'days'
		  break;
		case 'w':
		  logPeriod = 'weeks'
		  break;
		case 'm':
		  logPeriod = 'months'
		  break;
		default:
	  }
	  switch (dataPeriod) {
		case 'h':
		  dataPeriod = 'hours'
		  break;
		case 'd':
		  dataPeriod = 'days'
		  break;
		case 'w':
		  dataPeriod = 'weeks'
		  break;
		case 'm':
		  dataPeriod = 'months'
		  break;
		default:
	  }
	  console.log(`Removing Logs older than ${logInterval} ${logPeriod}`)
	  console.log(`Removing Backups older than ${dataInterval} ${dataPeriod}`)

	  files.forEach(file => {
		console.log(file);
		var ext = file.split('.').pop();
		var parts = file.split('_')
		if (ext == "log") {
		  var date = moment(parts[0], 'YYYYMMDD')
		  var diff = today.diff(date, logPeriod)
		  if (diff > logInterval) {
			removeFile(process.env.SAVELOCATION+file)
		  }
		}
		if (ext == "gz") {
		  var date = moment(parts[0], 'YYYYMMDD')
		  var diff = today.diff(date, dataPeriod)
		  if (diff > dataInterval) {
			removeFile(process.env.SAVELOCATION+file)
		  }
		}
	  });
	});

	function removeFile(path) {
	  fs.unlink(path, (err) => {
		if (err) {
		  console.error(err)
		  return
		}
		//file removed
	  })
	}
} else {
	var db = workerData.db
	let backupProcess = spawn('mongodump', [
		`--uri="${process.env.URI}"`,
		`--archive=${process.env.SAVELOCATION}${moment().format('YYYYMMDD_HHmmss')}_${db}.gz`,
		`--db=${db}`,
		'--gzip'
	]);

	  var logStream = fs.createWriteStream(`${process.env.SAVELOCATION}${moment().format('YYYYMMDD_HHmmss')}_${db}.log`, {flags: 'a'});

	  backupProcess.stdout.pipe(logStream);
	  backupProcess.stderr.pipe(logStream);

	  backupProcess.on('exit', (code, signal) => {
		if(code) {
		  console.log('Backup process exited with code ', code);
		  notifier.notify({
			title: `MongoDB Automated Backup (${db})`,
			message: 'Error During Backup! See logs...',
			icon: path.join(__dirname, 'icon.png')
		  });
		} else if (signal) {
		  console.error('Backup process was killed with singal ', signal);
		  notifier.notify({
			title: `MongoDB Automated Backup (${db})`,
			message: 'Error During Backup! See logs...',
			icon: path.join(__dirname, 'icon.png')
		  });
		} else {
		  console.log(`Successfully backed up the database (${db})`)
		  notifier.notify({
			title: `MongoDB Automated Backup (${db})`,
			message: 'Backup Complete!',
			icon: path.join(__dirname, 'icon.png')
		  });
		}
	  });
}
