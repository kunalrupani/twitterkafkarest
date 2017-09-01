var Twit = require('twit');
const kafka = require('kafka-node');
const express = require('express');
const app = express();

const {twitterconfig} = require('./twitterconfig');
const {kafkaconfig} = require('./kafkaconfig');

const bodyParser = require('body-parser');

app.use(bodyParser.json());

var T = new Twit({
    consumer_key: twitterconfig.consumer_key,
    consumer_secret: twitterconfig.consumer_secret,
    access_token: twitterconfig.access_token_key,
    access_token_secret: twitterconfig.access_token_secret,
    timeout_ms:           60*1000,  
  });

var Producer = kafka.Producer;
var kclient = new kafka.Client(kafkaconfig.connectionstring);
var producer = new Producer(kclient);

var payloads = [
    { topic: 'bigdata003-topicdemo', messages: ''}
];

var hashtag = "oracle";

let stream = T.stream('statuses/filter', { track: hashtag })
tweetstream(hashtag);

//Environment Parameters
var port = Number(process.env.PORT || 8080);

 // Server GET
app.get('/', (req, res) => {
    console.log("Root");
    res.send("Success !");  
});

app.get('/stop', (req, res) => {
  console.log("********STOP*******");
  stream.stop();
  res.send("Stopped !");  
});

app.get('/hashtag/:id', (req, res) => {
    
    console.log("Old filter: "+ hashtag);
    hashtag = req.params.id;
    console.log("New filter request: "+ hashtag); 
    tweetstream(hashtag);
    res.send("Filter Applied: " + hashtag);
    });

 // server listen
 app.listen(port, function() {
    console.log("Listening on " + port);
  });

function tweetstream(hastag){
  stream.stop();
  stream = T.stream('statuses/filter', { track: hashtag })
  stream.on('tweet', function (tweet) {
   console.log(tweet.text);
   payloads[0].messages = JSON.stringify(tweet);
   producer.send(payloads, function (err, data) {
   console.log("Sent to EventHub !");});
   });


}
  