import unittest

from summarize_hot_path import collection_counters


class CollectionCountersTest(unittest.TestCase):
    def test_counts_proof_restarts_and_preserves_the_historical_counter(self):
        operations = [
            {'stages': {'rt.gc.restart.proof': {'units': 2},
                        'rt.gc.restart.fence': {'units': 3}}},
            {'stages': {'rt.gc.restart.lease': {'units': 5},
                        'rt.memo.eviction': {'units': 7}}},
            {'stages': {}},
        ]
        counts = collection_counters(operations)
        self.assertEqual(counts['rt.gc.restart.proof_count'], 2)
        self.assertEqual(counts['rt.gc.restart.lease_count'], 5)
        self.assertEqual(counts['rt.gc.restart.fence_count'], 3)
        self.assertEqual(counts['rt.memo.eviction_count'], 7)
        self.assertEqual(counts['rt.gc.restart.epoch_count'], 0)
        self.assertEqual(counts['rt.gc.restart.page_count'], 0)


if __name__ == '__main__':
    unittest.main()
