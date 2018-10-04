var app  = require('express')()
var bodyParser  = require('body-parser');
var cors = require('cors');
var busboy = require('connect-busboy');
var mysql = require('mysql');
var admin = require('firebase-admin');
var tmi = require('tmi.js');
var CronJob = require('cron').CronJob;
var serviceAccount = require('./firebase-config.json');
const request = require('request');

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: "https://barcavelha-3c454.firebaseio.com"
});

//bodyparser needs
app.use(bodyParser.urlencoded({
  extended: true
}));

//bodyParse/Cors/Busboy
app.use(bodyParser.json({ extended: true }));
app.use(cors());
app.use(busboy()); 

//database
var pool  = mysql.createPool({
	connectionLimit : 200,
	host     : 'g8mh6ge01lu2z3n1.cbetxkdyhwsb.us-east-1.rds.amazonaws.com',
	port : '3306',
	user     : 'ghy6trmydb22z0e2',
	password : 'ife8u8clqe0rjd74',
	database: 'ay7h56yxux99uzop'
  });

var send_push_online = true;

//Funcao que deleta os pontos que não foram clamados pelos viewers no applicativo
function deleta(error, response, body) {	
	pool.getConnection(function(err, connection) {	
		var string = "delete from pontos where data_pontuacao < (SELECT NOW() - INTERVAL 1 DAY);";
		console.log(string);		
		connection.query(string, function(err, data2) {
			if (err){
				var error = {};
				error.type = 1;
				error.msg = err;
			}
		});
	connection.release();
	});	
}

//Funcao que detecta quais Streamers estao online
function callbackViewers(error, response, body) {
  if (!error && response.statusCode == 200) {
	var info = JSON.parse(body);	
	pool.getConnection(function(err, connection) {
	var dt = new Date();
	var utcDate = dt.toUTCString(); 
	console.log("Iniciando Pontuação "+ utcDate);
	info.chatters.viewers.map(function(chatter) { 				
		var string1 = "INSERT INTO pontos (user_twitch, data_pontuacao) VALUES ('"+chatter+"', now())";				
		//console.log(string1);
		connection.query(string1, function(err, data2) {
			if (err){
				var error = {};
				error.type = 1;
				error.msg = err;
				connection.release();
				return res.jsonp(error);
			}
		});	
	});
	connection.release();
	});
  }
}

//Funcao que detecta se o Stream esta online, se ele estive. Pontua todos que estão no chat
function callbackStreamer(error, response, body) { 
  if (!error && response.statusCode == 200) {
	var info = JSON.parse(body);	
	if(info.stream==='null'){
		//Streamer estava online, entao a proxima vez que ficar online, vai mandar a push
		send_push_online = true
	}else{
		//O Streamer esta online e so envia a push se o backend esta executando pela primeira vez isso, ou se
		//o Streamer estava offline na ultima vez que executou esse CRON
		if(send_push_online){
			//chama a push
			var topic = 'barca_velha';	
			var message = {
			  notification: {
				title: 'Barca Velha ',
				body: 'Estou Online Marujos!!!!!!'
			  },
			   topic: topic
			};			
			admin.messaging().send(message)
			.then((response) => {

			console.log('Successfully sent message:', response);
			})
			.catch((error) => {
			console.log('Error sending message:', error);
			});
			send_push_online = false;
		}
		//Fazer Pontuacao de Todos os Chatters online	
		var options = {
		  url: 'https://tmi.twitch.tv/group/user/gratis150ml/chatters',
		  headers: {
			'Client-Id': '67lx0lep7n77mav8o20ncg1sch26ke'
		  }
		};	
		request(options, callbackViewers);	
	}	
  }
}


var streamerOnline = new CronJob({
  cronTime: '0 */10 * * * *',
  onTick: function() {
	var options = {
	  url: 'https://api.twitch.tv/kraken/streams/gratis150ml',
	  headers: {
		'Client-Id': '67lx0lep7n77mav8o20ncg1sch26ke'
	  }
	};	
	request(options, callbackStreamer);	
  },
  start: false,
  timeZone: 'America/Sao_Paulo'
});

var deletaPontos = new CronJob({
  cronTime: '00 00 10 * * 0-6',
  onTick: function() {
	deleta();
  },
  start: false,
  timeZone: 'America/Sao_Paulo'
});

streamerOnline.start();
deletaPontos.start();

//configuracao para o heroku
app.listen(process.env.PORT || 5000)

console.log('Iniciando Pontuador');