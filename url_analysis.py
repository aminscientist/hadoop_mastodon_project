from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import re

class ContentAnalysisJob(MRJob):
    def map_extract_urls(self, _, line):
        toot = json.loads(line)
        content = toot.get("content", "")
        
        # Use regex to find URLs in the content
        urls = re.findall(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', content)
        
        # Emit key-value pairs for each URL found
        for url in urls:
            yield url, 1

    def reduce_count_urls(self, url, counts):
        # Aggregate the counts for each URL
        total_mentions = sum(counts)
        
        # Emit key-value pairs with the URL and the total count
        yield url, total_mentions

    def steps(self):
        return [
            MRStep(mapper=self.map_extract_urls, reducer=self.reduce_count_urls)
        ]

if __name__ == '__main__':
    ContentAnalysisJob().run()
