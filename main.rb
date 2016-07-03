require 'sinatra'
require 'redis'
require 'json'

# $redis = Redis.new(:host => "localhost", :port => 6379)
$redis = Redis.new(:host => "pub-redis-18583.us-east-1-3.4.ec2.garantiadata.com", :port => 18583)

get '/object/:key' do
  ts = params['timestamp'] || Time.now.to_i
  key = params['key']
  return 601, custom_error("No key found") if key == "" || key.nil?
  latest = $redis.zrevrangebyscore params['key'], ts, 0, :limit => [0, 1]
  return 602, custom_error("No value found with key: #{key}") if latest.empty?
  return latest
end

post '/object' do
  begin
    payload = JSON.parse(request.body.read)
    return 603, custom_error("Bad request body") if payload.nil? || payload.empty?
  rescue JSON::ParserError => e
    return 603, custom_error("Bad request body")
  end
  key, value = payload.first
  ts = Time.now.to_i
  latest = $redis.zrevrangebyscore key, ts, 0, :limit => [0, 1]
  $redis.zadd key, ts, value unless latest == value
end

not_found do
  'Sorry, this route is not available'
end

def custom_error str
  {"error" => str}.to_json
end
