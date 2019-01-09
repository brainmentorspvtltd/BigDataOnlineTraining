# we want to perform a calculation that involves multiple steps
# what is we wanted to count the words in documents stored in database
# find the most common word being used..

# 1. Map our text to a mapper that outputs pairs of (word,1)
# 2. Combine the pairs using the word as key (optional)
# 3. Reduce the pairs using the word as key
# 4. Find the word with max count

from mrjob.job import MRJob
from mrjob.step import MRStep
from heapq import nlargest
import re

WORD_re = re.compile(r'[\w]+')

class MRMostUsedWords(MRJob):

    def steps(self):
        return [
            MRStep(mapper = self.mapper_get_words,
                    combiner = self.combiner_count_words,
                    reducer = self.reducer_count_words),
            # MRStep(reducer = self.reducer_find_max_word)
            MRStep(reducer = self.reducer_find_top_10)
        ]
    
    def mapper_get_words(self, _, line):
        # yield each word in the line
        for word in WORD_re.findall(line):
            yield (word.lower(), 1)
    
    def combiner_count_words(self, word, counts):
        # sum the words we've seen so far
        yield (word, sum(counts))
    
    def reducer_count_words(self, word, counts):
        # send all (num_occurrences, word) pairs to same reducer
        # num_occurrences is so we can easily use max()
        yield None, (sum(counts), word)
    
    # def reducer_find_max_word(self, _, word_count_pairs):
    #     yield max(word_count_pairs)

    def reducer_find_top_10(self, _, word_count_pairs):
        for val in nlargest(10, word_count_pairs):
            yield val

if __name__ == "__main__":
    MRMostUsedWords.run()