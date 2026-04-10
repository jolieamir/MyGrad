"""
Apriori Algorithm Module

Primary: PySpark distributed SON (Savasere-Omiecinski-Navathe) algorithm
Fallback: Pure-Python Apriori implementation (single-machine)
"""

import math
from itertools import combinations
from collections import defaultdict


class AprioriMiner:
    def __init__(self, min_support=0.01, min_confidence=0.5):
        self.min_support = min_support
        self.min_confidence = min_confidence

    def run_distributed(self, transactions, progress_callback=None):
        """Run Apriori. Uses PySpark SON if available, else pure Python.

        Args:
            transactions: list[list[str]]
            progress_callback: optional fn(pct, detail) called with 0-100 progress
        """
        self._cb = progress_callback or (lambda pct, detail=None: None)

        try:
            from data_mining.spark_session import get_spark
            spark = get_spark()
            if spark is not None:
                return self._run_pyspark_son(spark, transactions)
        except Exception as e:
            print(f"[Apriori] PySpark failed, using pure Python: {e}")

        return self._run_pure_python(transactions)

    # ------------------------------------------------------------------
    # PySpark SON implementation
    # ------------------------------------------------------------------
    def _run_pyspark_son(self, spark, transactions):
        sc = spark.sparkContext
        rdd = sc.parallelize(transactions, numSlices=max(4, sc.defaultParallelism))
        total_count = rdd.count()

        if total_count == 0:
            return {'frequent_itemsets': [], 'association_rules': []}

        min_sup = self.min_support

        # Phase 1: local frequent itemsets
        candidates_rdd = rdd.mapPartitions(
            lambda part: AprioriMiner._apriori_partition(part, min_sup, total_count)
        )
        candidates = candidates_rdd.distinct().collect()

        if not candidates:
            return {'frequent_itemsets': [], 'association_rules': []}

        bc_candidates = sc.broadcast(candidates)

        # Phase 2: global counting
        global_counts = (
            rdd
            .flatMap(lambda txn: [
                (c, 1) for c in bc_candidates.value if c <= set(txn)
            ])
            .reduceByKey(lambda a, b: a + b)
            .filter(lambda x: x[1] / total_count >= min_sup)
            .collect()
        )
        bc_candidates.unpersist()

        itemset_support = {}
        frequent_itemsets = []
        for itemset, count in global_counts:
            support = count / total_count
            itemset_support[itemset] = support
            frequent_itemsets.append({
                'items': sorted(list(itemset)),
                'freq': count,
                'support': round(support, 6),
            })
        frequent_itemsets.sort(key=lambda x: x['support'], reverse=True)

        association_rules = self._generate_rules(itemset_support)

        return {'frequent_itemsets': frequent_itemsets, 'association_rules': association_rules}

    @staticmethod
    def _apriori_partition(partition, global_support, total_count):
        """Local Apriori for a single partition (SON Phase 1)."""
        partition = list(partition)
        n = len(partition)
        if n == 0:
            return iter([])

        local_support = math.ceil(global_support * n)

        item_counts = {}
        for txn in partition:
            for item in txn:
                item_counts[item] = item_counts.get(item, 0) + 1

        current_freq = {
            frozenset([item]) for item, count in item_counts.items()
            if count >= local_support
        }
        all_freq = set(current_freq)

        k = 2
        while current_freq:
            candidates = set()
            freq_list = list(current_freq)
            for i in range(len(freq_list)):
                for j in range(i + 1, len(freq_list)):
                    union = freq_list[i] | freq_list[j]
                    if len(union) == k:
                        candidates.add(union)

            candidate_counts = {}
            for txn in partition:
                txn_set = set(txn)
                for c in candidates:
                    if c <= txn_set:
                        candidate_counts[c] = candidate_counts.get(c, 0) + 1

            current_freq = {c for c, cnt in candidate_counts.items() if cnt >= local_support}
            all_freq |= current_freq
            k += 1

        return iter(all_freq)

    # ------------------------------------------------------------------
    # Pure-Python Apriori
    # ------------------------------------------------------------------
    def _run_pure_python(self, transactions):
        total = len(transactions)
        if total == 0:
            return {'frequent_itemsets': [], 'association_rules': []}

        min_count = max(1, int(self.min_support * total))
        import array

        # Phase 1: Count item frequencies (10%)
        self._cb(5, 'Counting item frequencies...')
        item_counts = defaultdict(int)
        for txn in transactions:
            for item in set(txn):
                item_counts[item] += 1

        freq_items = {item for item, cnt in item_counts.items() if cnt >= min_count}
        self._cb(10, f'{len(freq_items)} frequent items found')

        # Phase 2: Map items to integers and build compact inverted index
        # Uses sorted arrays instead of sets — ~4x less memory
        self._cb(15, 'Building inverted index...')
        item_to_id = {item: i for i, item in enumerate(sorted(freq_items))}
        id_to_item = {i: item for item, i in item_to_id.items()}

        inverted = {}  # int_id -> sorted array of txn indices
        for i, txn in enumerate(transactions):
            for item in txn:
                if item in item_to_id:
                    iid = item_to_id[item]
                    if iid not in inverted:
                        inverted[iid] = []
                    inverted[iid].append(i)

        # Convert lists to frozensets for fast intersection
        inv_sets = {iid: frozenset(indices) for iid, indices in inverted.items()}
        del inverted  # free memory
        self._cb(20, 'Inverted index built')

        # Phase 3: Frequent 1-itemsets (25%)
        itemset_support = {}
        frequent_itemsets = []
        freq_1_map = {}

        for item in freq_items:
            count = item_counts[item]
            fs = frozenset([item_to_id[item]])
            freq_1_map[fs] = count
            support = count / total
            itemset_support[fs] = support
            frequent_itemsets.append({
                'items': [item],
                'freq': count,
                'support': round(support, 6),
            })
        self._cb(25, f'{len(freq_1_map)} frequent 1-itemsets')

        # Phase 4: Generate k-itemsets using integer IDs (25% -> 75%)
        current_freq = dict(freq_1_map)
        k = 2

        while current_freq:
            # Generate candidates
            candidates = set()
            freq_list = list(current_freq.keys())
            for i in range(len(freq_list)):
                for j in range(i + 1, len(freq_list)):
                    union = freq_list[i] | freq_list[j]
                    if len(union) == k:
                        candidates.add(union)

            if not candidates:
                break

            # Count using inverted index intersection (integer IDs)
            next_freq = {}
            for candidate in candidates:
                int_ids = list(candidate)
                # Start with smallest set for faster intersection
                int_ids.sort(key=lambda x: len(inv_sets.get(x, frozenset())))
                common_txns = inv_sets.get(int_ids[0], frozenset())
                for iid in int_ids[1:]:
                    common_txns = common_txns & inv_sets.get(iid, frozenset())
                    if len(common_txns) < min_count:
                        break

                count = len(common_txns)
                if count >= min_count:
                    support = count / total
                    itemset_support[candidate] = support
                    # Convert IDs back to item names for output
                    item_names = sorted([id_to_item[iid] for iid in candidate])
                    frequent_itemsets.append({
                        'items': item_names,
                        'freq': count,
                        'support': round(support, 6),
                    })
                    next_freq[candidate] = count

            current_freq = next_freq
            k_pct = min(75, 25 + k * 12)
            self._cb(k_pct, f'Level k={k}: {len(candidates)} candidates, '
                            f'{len(next_freq)} frequent, '
                            f'{len(frequent_itemsets)} total itemsets')
            k += 1

        del inv_sets  # free memory

        frequent_itemsets.sort(key=lambda x: x['support'], reverse=True)
        self._cb(95, f'{len(frequent_itemsets)} itemsets found')

        print(f"[Apriori] Found {len(frequent_itemsets)} frequent itemsets (pure Python)")

        self._cb(100, 'Apriori complete')
        # Apriori focuses on frequent itemsets only — no association rules
        return {'frequent_itemsets': frequent_itemsets, 'association_rules': []}

    # ------------------------------------------------------------------
    # Shared rule generation
    # ------------------------------------------------------------------
    def _generate_rules(self, itemset_support):
        rules = []
        for itemset, support in itemset_support.items():
            if len(itemset) < 2:
                continue
            items = list(itemset)
            for i in range(1, len(items)):
                for ant_tuple in combinations(items, i):
                    antecedent = frozenset(ant_tuple)
                    consequent = itemset - antecedent

                    ant_support = itemset_support.get(antecedent)
                    con_support = itemset_support.get(consequent)

                    if not ant_support or ant_support == 0:
                        continue

                    confidence = support / ant_support
                    if confidence < self.min_confidence:
                        continue

                    lift = confidence / con_support if con_support and con_support > 0 else 0

                    rules.append({
                        'antecedents': sorted(list(antecedent)),
                        'consequents': sorted(list(consequent)),
                        'confidence': round(confidence, 6),
                        'support': round(support, 6),
                        'lift': round(lift, 4),
                    })

        rules.sort(key=lambda x: x['lift'], reverse=True)
        return rules

    def close(self):
        pass
