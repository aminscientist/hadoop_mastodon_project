from mrjob.job import MRJob
from mrjob.step import MRStep
import json
from datetime import datetime

class UserGrowthAnalysisJob(MRJob):

    def mapper_extract_user_creation_date(self, _, line):
        try:
            data = json.loads(line.strip())

            if "account" in data and "created_at" in data["account"]:
                created_at = data["account"]["created_at"]
                user_creation_date = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%S.%fZ").date()
                yield user_creation_date, 1

        except json.JSONDecodeError:
            # Handle JSON decoding errors
            pass

    def reducer_count_user_creation_dates(self, user_creation_date, counts):
        total_users_created = sum(counts)
        yield user_creation_date, total_users_created

    def steps(self):
        return [
            MRStep(mapper=self.mapper_extract_user_creation_date, reducer=self.reducer_count_user_creation_dates),
        ]

if __name__ == '__main__':
    UserGrowthAnalysisJob().run()
