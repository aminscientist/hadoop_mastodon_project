from mrjob.job import MRJob
from mrjob.step import MRStep
import json

class TagsMentionsAnalysisJob(MRJob):
    def map_extract_tags_mentions(self, _, line):
        toot = json.loads(line)
        
        # Extract tags from the content
        tags = toot.get("tags", [])
        for tag in tags:
            yield tag, 1

        # Extract mentions from the content
        mentions = toot.get("mentions", [])
        for mention in mentions:
            yield mention, 1

    def reduce_count_tags_mentions(self, tag_or_mention, counts):
        # Aggregate the counts for each tag or mention
        total_occurrences = sum(counts)
        
        # Emit key-value pairs with the tag or mention and the total count
        yield tag_or_mention, total_occurrences

    def steps(self):
        return [
            MRStep(mapper=self.map_extract_tags_mentions, reducer=self.reduce_count_tags_mentions)
        ]

if __name__ == '__main__':
    TagsMentionsAnalysisJob().run()
