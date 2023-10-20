from hdfs import InsecureClient
import json

# Initialize an HDFS client
hdfs_client = InsecureClient('http://localhost:9870', user='hadoop')

# Specify the HDFS path to the file you want to read
hdfs_path = '/timeline/timeline_2023-10-20.json'  # Update this path

# Mapper
def map_to_followers(data):
    for line in data:
        try:
            # Load the JSON data from the line
            post = json.loads(line)
            account_id = post.get('account_id')
            account_followers_count = post.get('account_followers_count')
            if account_id is not None and account_followers_count is not None:
                # Output the user and their number of followers
                print(f"User: {account_id}, Followers: {account_followers_count}")
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
        except KeyError as e:
            print(f"Key not found: {e}")

# Read the data from the specified HDFS file
with hdfs_client.read(hdfs_path, encoding='utf-8') as reader:
    data = reader.read().splitlines()

# Apply the mapper function to the data
map_to_followers(data)
