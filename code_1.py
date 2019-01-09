import re
from mrjob.job import MRJob

WORD_RE = re.compile(r'[\w]+')

class MRWordFreqCount(MRJob):

    # def mapper(self, _, line):
    #     for word in WORD_RE.findall(line):
    #         yield (word.lower(), 1)

    # def reducer(self,word,counts):
    #     yield (word, sum(counts))

    def mapper(self, _, line):
        yield "chars", len(line)
        yield "words", len(line.split())
        yield "lines", 1

    def reducer(self,key, values):
        yield key, sum(values)

if __name__ == "__main__":
    MRWordFreqCount.run()