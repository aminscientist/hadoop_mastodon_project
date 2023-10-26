from mrjob.job import MRJob
from mrjob.step import MRStep
import json

class MediaEngagementAnalysisJob(MRJob):
    def map_check_media_attachments(self, _, line):
        toot = json.loads(line)
        has_media = 1 if toot.get("media_attachments") else 0
        
        # Emit key-value pairs for each toot indicating if it has media
        yield has_media, 1

    def reduce_count_media_engagement(self, has_media, counts):
        # Aggregate the counts for toots with and without media
        total_occurrences = sum(counts)
        
        # Emit key-value pairs with the indicator and the total count
        yield f'Toots with Media: {has_media}', total_occurrences

    def steps(self):
        return [
            MRStep(mapper=self.map_check_media_attachments, reducer=self.reduce_count_media_engagement)
        ]

if __name__ == '__main__':
    MediaEngagementAnalysisJob().run()
