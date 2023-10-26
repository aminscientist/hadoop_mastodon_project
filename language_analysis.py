from mrjob.job import MRJob
from mrjob.step import MRStep
import json

class LanguageAnalysisJob(MRJob):
    def map_extract_language(self, _, line):
        toot = json.loads(line)
        language = toot.get("language", "unknown")  # Assume "unknown" if language is not present
        
        # Emit key-value pairs for each language found
        yield language, 1

    def reduce_count_languages(self, language, counts):
        # Aggregate the counts for each language
        total_occurrences = sum(counts)
        
        # Emit key-value pairs with the language and the total count
        yield language, total_occurrences

    def steps(self):
        return [
            MRStep(mapper=self.map_extract_language, reducer=self.reduce_count_languages)
        ]

if __name__ == '__main__':
    LanguageAnalysisJob().run()
