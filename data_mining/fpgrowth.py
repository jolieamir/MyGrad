"""
FP-Growth Algorithm Module

Primary: PySpark MLlib FPGrowth (distributed, if available)
Fallback: Pure-Python FP-Growth implementation (single-machine)
"""

from collections import defaultdict, OrderedDict


class FPGrowthMiner:
    def __init__(self, min_support=0.01, min_confidence=0.5):
        self.min_support = min_support
        self.min_confidence = min_confidence

    def run_distributed(self, transactions, progress_callback=None):
        """Run FP-Growth. Uses PySpark if available, else pure Python.

        Args:
            transactions: list[list[str]]
            progress_callback: optional fn(pct, detail) called with 0-100 progress

        Returns:
            dict with keys 'frequent_itemsets' and 'association_rules'
        """
        self._cb = progress_callback or (lambda pct, detail=None: None)

        # Try PySpark first
        try:
            from data_mining.spark_session import get_spark
            spark = get_spark()
            if spark is not None:
                return self._run_pyspark(spark, transactions)
        except Exception as e:
            print(f"[FP-Growth] PySpark failed, using pure Python: {e}")

        # Fallback: pure Python
        return self._run_pure_python(transactions)

    # ------------------------------------------------------------------
    # PySpark implementation
    # ------------------------------------------------------------------
    def _run_pyspark(self, spark, transactions):
        from pyspark.sql import Row
        from pyspark.ml.fpm import FPGrowth as SparkFPGrowth

        total = len(transactions)
        rows = [Row(items=list(t)) for t in transactions]
        df = spark.createDataFrame(rows)
        df = df.repartition(max(4, spark.sparkContext.defaultParallelism))
        df.cache()

        fp = SparkFPGrowth(itemsCol="items", minSupport=self.min_support,
                           minConfidence=self.min_confidence)
        model = fp.fit(df)

        raw_itemsets = model.freqItemsets.collect()
        support_lookup = {}
        frequent_itemsets = []
        for row in raw_itemsets:
            freq = row['freq']
            support = freq / total
            support_lookup[frozenset(row['items'])] = support
            frequent_itemsets.append({
                'items': sorted(row['items']),
                'freq': freq,
                'support': round(support, 6),
            })
        frequent_itemsets.sort(key=lambda x: x['support'], reverse=True)

        raw_rules = model.associationRules.collect()
        association_rules = []
        for row in raw_rules:
            ant_support = support_lookup.get(frozenset(row['antecedent']), 0)
            confidence = float(row['confidence'])
            association_rules.append({
                'antecedents': sorted(row['antecedent']),
                'consequents': sorted(row['consequent']),
                'confidence': round(confidence, 6),
                'support': round(confidence * ant_support, 6),
                'lift': round(float(row['lift']), 4),
            })
        association_rules.sort(key=lambda x: x['lift'], reverse=True)

        df.unpersist()
        return {'frequent_itemsets': frequent_itemsets, 'association_rules': association_rules}

    # ------------------------------------------------------------------
    # Pure-Python FP-Growth implementation
    # ------------------------------------------------------------------
    def _run_pure_python(self, transactions):
        total = len(transactions)
        if total == 0:
            return {'frequent_itemsets': [], 'association_rules': []}

        min_count = max(1, int(self.min_support * total))

        # Phase 1: Count item frequencies (10%)
        self._cb(5, 'Counting item frequencies...')
        item_counts = defaultdict(int)
        for txn in transactions:
            for item in set(txn):
                item_counts[item] += 1

        freq_items = {item for item, count in item_counts.items() if count >= min_count}
        self._cb(10, f'{len(freq_items)} frequent items found')

        # Map items to integers (saves memory in tree nodes)
        item_to_id = {}
        id_to_item = {}
        for i, item in enumerate(sorted(freq_items, key=lambda x: -item_counts[x])):
            item_to_id[item] = i
            id_to_item[i] = item

        # Phase 2: Build filtered transactions with integer IDs (20%)
        sorted_txns = []
        for txn in transactions:
            filtered = [item_to_id[item] for item in txn if item in item_to_id]
            filtered.sort()  # IDs already sorted by frequency
            if filtered:
                sorted_txns.append(filtered)
        del transactions  # free original list
        self._cb(20, 'Transactions filtered and sorted')

        # Phase 3: Build FP-Tree (35%)
        self._cb(25, 'Building FP-Tree...')
        root, header_table = self._build_fp_tree(sorted_txns, min_count)
        self._cb(35, f'FP-Tree built ({len(header_table)} nodes)')

        # Phase 4: Mine frequent patterns (70%)
        self._cb(40, 'Mining frequent patterns...')
        patterns = {}
        self._mine_tree(header_table, min_count, set(), patterns)
        self._cb(70, f'{len(patterns)} patterns mined')

        del sorted_txns  # free memory

        # Build support lookup (needed internally for rule generation)
        itemset_support = {}
        for itemset_tuple, count in patterns.items():
            name_set = frozenset(id_to_item[iid] for iid in itemset_tuple)
            itemset_support[name_set] = count / total
        del patterns  # free memory

        # Include single items for rule denominators (antecedent/consequent support)
        for item, count in item_counts.items():
            if count >= min_count:
                fs = frozenset([item])
                if fs not in itemset_support:
                    itemset_support[fs] = count / total

        self._cb(70, f'{len(itemset_support)} support values computed')

        # Generate association rules (FP-Growth focuses on rules only)
        self._cb(80, 'Generating association rules...')
        association_rules = self._generate_rules(itemset_support)
        association_rules.sort(key=lambda x: x['lift'], reverse=True)
        self._cb(95, f'{len(association_rules)} rules generated')

        print(f"[FP-Growth] Found {len(association_rules)} association rules (pure Python)")

        self._cb(100, 'FP-Growth complete')
        # FP-Growth focuses on association rules only — no frequent itemsets
        return {'frequent_itemsets': [], 'association_rules': association_rules}

    def _build_fp_tree(self, transactions, min_count):
        root = _FPNode(None, None)
        header_table = {}

        for txn in transactions:
            current = root
            for item in txn:
                child = current.children.get(item)
                if child is None:
                    child = _FPNode(item, current)
                    current.children[item] = child
                    # Update header table
                    if item in header_table:
                        last = header_table[item]
                        while last.next is not None:
                            last = last.next
                        last.next = child
                    else:
                        header_table[item] = child
                child.count += 1
                current = child

        return root, header_table

    def _mine_tree(self, header_table, min_count, prefix, patterns):
        # Sort items by frequency (ascending for bottom-up mining)
        sorted_items = sorted(header_table.keys(),
                              key=lambda x: self._count_support(header_table[x]))

        for item in sorted_items:
            new_prefix = prefix | {item}
            support = self._count_support(header_table[item])
            if support >= min_count:
                patterns[tuple(sorted(new_prefix))] = support

                # Build conditional pattern base
                cond_patterns = []
                node = header_table[item]
                while node is not None:
                    prefix_path = []
                    parent = node.parent
                    while parent is not None and parent.item is not None:
                        prefix_path.append(parent.item)
                        parent = parent.parent
                    if prefix_path:
                        for _ in range(node.count):
                            cond_patterns.append(prefix_path)
                    node = node.next

                # Build conditional FP-tree
                if cond_patterns:
                    cond_root, cond_header = self._build_fp_tree(cond_patterns, min_count)
                    if cond_header:
                        self._mine_tree(cond_header, min_count, new_prefix, patterns)

    @staticmethod
    def _count_support(node):
        count = 0
        while node is not None:
            count += node.count
            node = node.next
        return count

    def _generate_rules(self, itemset_support):
        from itertools import combinations
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

        return rules

    def close(self):
        pass


class _FPNode:
    """Node in an FP-Tree."""
    __slots__ = ['item', 'count', 'parent', 'children', 'next']

    def __init__(self, item, parent):
        self.item = item
        self.count = 0
        self.parent = parent
        self.children = {}
        self.next = None  # Link to next node with same item
