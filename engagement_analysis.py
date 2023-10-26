from mrjob.job import MRJob
from mrjob.step import MRStep
import json

class EngagementAnalysisJob(MRJob):
    def mapper_extract_stats(self, _, line):
        try:
            data = json.loads(line.strip())

            if "account" in data and "id" in data["account"]:
                user_id = data["account"]["id"]
                replies_count = data.get("replies_count", 0)
                reblogs_count = data.get("reblogs_count", 0)
                favourites_count = data.get("favourites_count", 0)
                followers_count = data["account"].get("followers_count", 0)

                yield user_id, (replies_count, reblogs_count, favourites_count, followers_count)

        except json.JSONDecodeError:
            # Handle JSON decoding errors
            pass

    def reducer_aggregate_stats(self, user_id, stats):
        total_replies = 0
        total_reblogs = 0
        total_favourites = 0
        total_followers = 0

        for replies, reblogs, favourites, followers_count in stats:
            total_replies += replies
            total_reblogs += reblogs
            total_favourites += favourites
            total_followers = followers_count  # Assuming followers_count is constant for a user

        engagement_rate = self.calculate_engagement_rate(total_favourites, total_reblogs, total_followers)
        yield user_id, engagement_rate

    def calculate_engagement_rate(self, favourites_count, reblogs_count, followers_count):
        # Use the formula you provided
        return (favourites_count + reblogs_count) / (followers_count if followers_count > 0 else 1)

    def steps(self):
        return [
            MRStep(mapper=self.mapper_extract_stats, reducer=self.reducer_aggregate_stats),
        ]

if __name__ == '__main__':
    EngagementAnalysisJob().run()
