var Twitter = require('twitter');
var elasticsearch = require('elasticsearch');

var twitterClient = new Twitter({
  consumer_key: '',
  consumer_secret: '',
  access_token_key: '',
  access_token_secret: ''
});

var elasticsearchClient = new elasticsearch.Client({
  host: ''
});

var q = "angular,angular2,reactjs,redux,javascript";
twitterClient.stream('statuses/filter', {track: q},  function(stream){
  stream.on('data', function(tweet) {
    hashtags = [];
    users = [];
    if (tweet.entities) {
      for (var i = 0; i < tweet.entities.hashtags.length; i++) {
        hashtags.push(tweet.entities.hashtags[i].text);
      }
      for (var j = 0; j < tweet.entities.user_mentions.length; j++) {
        users.push(tweet.entities.user_mentions[j].screen_name);
      }
    }

    var tweetObj = {
      created_at: new Date(tweet.created_at),
      id: tweet.id_str,
      text: tweet.text,
      user_id: tweet.user.id_str,
      user_screen_name: tweet.user.screen_name,
      user_name: tweet.user.name,
      hashtags: hashtags,
      users_mentioned: users,
      retweeted_from_id: tweet.retweeted_status ? tweet.retweeted_status.user.id_str : null,
      retweeted_from_screen_name: tweet.retweeted_status ? tweet.retweeted_status.user.screen_name : null,
      retweeted_from_name: tweet.retweeted_status ? tweet.retweeted_status.user._name : null,
    }
    console.log(tweetObj);
    postTweetToES(tweetObj);

    if (tweet.retweeted_status) {
      if (users.indexOf(tweet.retweeted_status.user.screen_name) < 0) {
        users.push(tweet.retweeted_status.user.screen_name);
      }
    }

    if (users.length > 0) {
      var user = {
        id: tweet.user.id_str,
        user_screen_name: tweet.user.screen_name,
        user_name: tweet.user.name
      }
      console.log(user, users);
      postUserToES(user, users);
    }

  });

  stream.on('error', function(error) {
    console.log("ERROR:", error);
  });
});

function postUserToES(user, associatedUsers) {
  elasticsearchClient.update({
    id: user.id,
    index: 'web_tech_users',
    type: 'users',
    body: {
      script: 'ctx._source.retweeted_users.plus(retweeted_users)',
      params: { retweeted_users: associatedUsers },
      upsert: {
        id: user.id,
        user_screen_name: user.user_screen_name,
        user_name: user.user_name,
        retweeted_users: associatedUsers
      }
    }
  }, function (error) {
    if (error) {
      console.trace(error);
    } else {
      console.log('User updated successfully');
    }
  });
}

function postTweetToES(tweet) {
  elasticsearchClient.create({
    id: tweet.id,
    index: 'web_tech_tweets',
    type: 'tweets',
    body: tweet
  }, function (error) {
    if (error) {
      console.trace(error);
    } else {
      console.log('Tweet added successfully');
    }
  });
}
