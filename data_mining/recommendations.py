"""
Recommendation Engine Module
Generates product recommendations from mining results (Apriori or FP-Growth).

Both algorithms produce the same output format:
  {
    'frequent_itemsets': [{'items': [...], 'freq': N, 'support': F}, ...],
    'association_rules': [{'antecedents': [...], 'consequents': [...],
                           'confidence': F, 'support': F, 'lift': F}, ...]
  }
"""

from collections import defaultdict
import json


class RecommendationEngine:
    def __init__(self):
        self.frequent_itemsets = []
        self.association_rules = []
        # product_name -> list of recommendation dicts
        self._recommendations = {}

    def load_mining_results(self, frequent_itemsets, association_rules=None):
        """Load results from either algorithm and build recommendations."""
        self.frequent_itemsets = frequent_itemsets or []
        self.association_rules = association_rules or []
        self._build_recommendations()

    # ------------------------------------------------------------------
    # Internal: build recommendation lookup from rules + itemsets
    # ------------------------------------------------------------------
    def _build_recommendations(self):
        recs = defaultdict(list)

        # Primary source: association rules (have confidence + lift)
        for rule in self.association_rules:
            for ant in rule['antecedents']:
                for con in rule['consequents']:
                    recs[ant].append({
                        'product': con,
                        'confidence': rule.get('confidence', 0),
                        'support': rule.get('support', 0),
                        'lift': rule.get('lift', 0),
                        'source': 'rule',
                    })

        # Secondary source: frequent itemsets (co-occurrence)
        for itemset_info in self.frequent_itemsets:
            items = itemset_info.get('items', [])
            support = itemset_info.get('support', 0)
            if len(items) < 2:
                continue
            for item in items:
                for other in items:
                    if other == item:
                        continue
                    # Only add if not already covered by a rule
                    existing = {r['product'] for r in recs[item]}
                    if other not in existing:
                        recs[item].append({
                            'product': other,
                            'confidence': 0,
                            'support': support,
                            'lift': 0,
                            'source': 'itemset',
                        })

        # Sort each product's recommendations: rules first (by lift), then itemsets (by support)
        for product in recs:
            recs[product].sort(
                key=lambda r: (r['lift'], r['confidence'], r['support']),
                reverse=True,
            )

        self._recommendations = dict(recs)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    @property
    def recommendations(self):
        """Dict mapping product_name -> list of recommendation dicts."""
        return self._recommendations

    def get_recommendations(self, product_name, top_n=5):
        """Get top-N recommendations for a single product."""
        return self._recommendations.get(product_name, [])[:top_n]

    def get_frequently_bought_together(self, product_name, top_n=4):
        """Get 'frequently bought together' items."""
        return self.get_recommendations(product_name, top_n)

    def get_cross_sell(self, cart_items, top_n=5):
        """Given a list of products in the cart, recommend items not already in cart."""
        cart_set = set(cart_items)
        scores = defaultdict(float)

        for item in cart_items:
            for rec in self._recommendations.get(item, []):
                product = rec['product']
                if product not in cart_set:
                    # Weight by lift (if available) else support
                    scores[product] += rec.get('lift', 0) or rec.get('support', 0)

        sorted_recs = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        return [{'product': p, 'score': round(s, 4)} for p, s in sorted_recs[:top_n]]

    def get_bundles(self, min_items=2, max_items=5, top_n=10):
        """Get product bundle suggestions from frequent itemsets."""
        bundles = []
        for info in self.frequent_itemsets:
            items = info.get('items', [])
            if min_items <= len(items) <= max_items:
                bundles.append({
                    'items': items,
                    'support': info.get('support', 0),
                    'freq': info.get('freq', 0),
                })
        bundles.sort(key=lambda x: x['support'], reverse=True)
        return bundles[:top_n]

    def get_rules_for_product(self, product_name, top_n=5):
        """Get association rules involving a specific product."""
        rules = []
        for rule in self.association_rules:
            if product_name in rule['antecedents'] or product_name in rule['consequents']:
                rules.append(rule)
        rules.sort(key=lambda x: x.get('lift', 0), reverse=True)
        return rules[:top_n]

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------
    def to_dict(self):
        return {
            'recommendations': self._recommendations,
            'frequent_itemsets': self.frequent_itemsets,
            'association_rules': self.association_rules,
        }

    def to_json(self):
        return json.dumps(self.to_dict(), indent=2)

    def load_from_json(self, json_str):
        data = json.loads(json_str)
        self.frequent_itemsets = data.get('frequent_itemsets', [])
        self.association_rules = data.get('association_rules', [])
        self._recommendations = data.get('recommendations', {})
