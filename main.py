import redis
import csv
from redis.commands.search.field import TextField, TagField, NumericField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.query import Query

class RedisAssignment:
    def __init__(self):
        self.redis = None

    def connect(self):
        print("\n==================== 1. Connecting to Redis ====================")
        try:
            self.redis = redis.Redis(
            host='redis-13610.crce206.ap-south-1-1.ec2.redns.redis-cloud.com', 
                port=13610,
                decode_responses=True,
                username="Raghav Soni G24AI1010",
                password="Hondacity@7571"
            )
            print("✅ Successfully connected to Redis.")
        except Exception as e:
            print("❌ Redis connection failed:", e)

    def load_users(self, file_path):
        print("\n==================== 2. Loading Users into Redis Hashes ====================")
        count = 0
        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                parts = line.strip().split('" "')
                parts = [p.replace('"', '') for p in parts]
                key = parts[0]
                user_data = dict(zip(parts[1::2], parts[2::2]))
                self.redis.hset(key, mapping=user_data)
                count += 1
        print(f"✅ Loaded {count} users using Redis Hashes (user:<id>).")

    def load_scores(self, file_path):
        print("\n==================== 2. Loading Scores into Redis Sorted Sets ====================")
        count = 0
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                user = row['user:id']
                score = int(row['score'])
                leaderboard = row['leaderboard']
                key = f"leaderboard:{leaderboard}"
                self.redis.zadd(key, {user: score})
                count += 1
        print(f"✅ Loaded {count} scores using Redis Sorted Sets (leaderboard:<id>).")

    def query1(self, usr):
        print("\n==================== 3. Query: Get All Attributes of a User ====================")
        key = f"user:{usr}"
        data = self.redis.hgetall(key)
        print(f"📄 Attributes for {key}:\n", data)
        return data

    def query2(self, usr):
        print("\n==================== 4. Query: Get Coordinates (Longitude & Latitude) ====================")
        key = f"user:{usr}"
        lon = self.redis.hget(key, 'longitude')
        lat = self.redis.hget(key, 'latitude')
        print(f"📍 Coordinates for {key}: Longitude = {lon}, Latitude = {lat}")
        return lon, lat

    def query3(self):
        print("\n==================== 5. Query: Get Keys & Last Names of Users with Even-Starting IDs ====================")
        even_users = []
        last_names = []
        cursor = 0
        while True:
            cursor, keys = self.redis.scan(cursor=cursor, match='user:*', count=100)
            for key in keys:
                user_id = key.split(':')[1]
                if user_id[0] in '02468':
                    even_users.append(key)
                    last_name = self.redis.hget(key, 'last_name')
                    last_names.append(last_name)
            if cursor == 0:
                break
        for k, l in zip(even_users, last_names):
            print(f"👤 {k} → Last Name: {l}")
        return even_users, last_names

    def query4(self):
        print("\n==================== 6. Query: Females in China or Russia with Latitude 40–46 ====================")
        try:
            self.redis.ft("user_idx").info()
        except:
            self.redis.ft("user_idx").create_index(
                fields=[
                    TextField("gender"),
                    TagField("country"),
                    NumericField("latitude")
                ],
                definition=IndexDefinition(prefix=["user:"], index_type=IndexType.HASH)
            )

        q = Query("(@gender:female) (@country:{China|Russia}) @latitude:[40 46]")
        results = self.redis.ft("user_idx").search(q)
        for doc in results.docs:
            print(f"🌏 {doc.id}: {doc.gender}, {doc.country}, Latitude={doc.latitude}")
        return results.docs

    def query5(self):
        print("\n==================== 7. Query: Top 10 Emails from Leaderboard 2 ====================")
        top_users = self.redis.zrevrange("leaderboard:2", 0, 9)
        emails = []
        for user_key in top_users:
            email = self.redis.hget(user_key, 'email')
            print(f"🏅 {user_key} → Email: {email}")
            emails.append(email)
        return emails


# === DRIVER ===
if __name__ == '__main__':
    r = RedisAssignment()
    r.connect()
    r.load_users('users.txt')
    r.load_scores('userscores.csv')
    r.query1(3)
    r.query2(3)
    r.query3()
    r.query4()
    r.query5()