import json
from mrjob.job import MRJob
from mrjob.step import MRStep


class MapReduce(MRJob):
    def mapper(self,key,value):
        data = json.loads(value)
        for i in data:
            username = i.get('account').get('username')
            followers = i.get('account').get('followers_count')
            language = i.get("language")
            yield(f"language:{language}",1)
            yield(f"followers:{username}",followers)
            yield(username, 1)

    def reducer(self,key,values):
        yield(key, sum(values))
        

    def steps(self):
        return [
            MRStep(
                mapper = self.mapper,
                reducer=self.reducer
            )
        ]
if __name__ == '__main__':
    MapReduce.run()
    
