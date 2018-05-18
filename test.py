import redis
import sys
import tzlocal

print(tzlocal.get_localzone())

# r=redis.Redis(host='localhost',port=6379,decode_responses=True, db=14)
# r=redis.StrictRedis(host='localhost',port=6379)

r=redis.Redis.from_url('redis://127.0.0.1:6379/14', decode_responses=True)

print(r.hgetall('symid.com'))
