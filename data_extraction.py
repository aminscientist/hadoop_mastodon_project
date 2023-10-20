from mastodon import Mastodon
from dotenv import load_dotenv
import os
import json
from datetime import datetime
from hdfs import InsecureClient

load_dotenv()
api_key = os.getenv("API_KEY")
api_base_url = "https://mastodon.social/"

def save_to_json_hdfs(data, hdfs_destination_path):
    # Serialize each record to a separate line
    json_str = '\n'.join(json.dumps(record, default=datetime_serializer) for record in data)
    json_bytes = json_str.encode('utf-8')

    # Create an InsecureClient to connect to HDFS
    hdfs_client = InsecureClient('http://localhost:9870/', user='hadoop')  # Adjust the port and user as needed

    # Write the JSON data to HDFS
    with hdfs_client.write(hdfs_destination_path, overwrite=True) as writer:
        writer.write(json_bytes)

    print(f"Data written to HDFS at: {hdfs_destination_path}")

def save_to_json(data, file_name):
    with open(file_name, "w") as outfile:
        for record in data:
            json.dump(record, outfile, default=datetime_serializer)
            outfile.write('\n')
    print(f'Data written to local at: {output_file_name}')

def datetime_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError("Type not serializable")

if __name__ == "__main__":
    mastodon = Mastodon(access_token=api_key, api_base_url=api_base_url)

    # Envoie d'un "toot" depuis le compte Mastodon
    #mastodon.toot('Tooting from Python using #mastodonpy !')

    # Récupération des 40 derniers "toots" de la timeline publique
    timeline = mastodon.timeline_public(limit=40)

    # Génération du chemin de destination HDFS avec la date d'extraction
    extraction_date = datetime.now().strftime("%Y-%m-%d")
    hdfs_destination_path = f'/timeline/timeline_{extraction_date}.json'
    output_file_name = f"timeline_{extraction_date}.json"


    # Sauvegarde des données dans le fichier JSON sur HDFS
    save_to_json_hdfs(timeline, hdfs_destination_path)
    save_to_json(timeline, output_file_name)

    print("done")
