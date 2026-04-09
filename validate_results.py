"""
Validate mining results against raw transaction data in SQL Server.

Manually recomputes support, confidence, and lift by counting transactions
directly — proving the algorithms produce correct results.
"""
import sys
import os
import json

os.environ['PYSPARK_PYTHON'] = sys.executable

from app import create_app, db
from app.models import MiningResult, Recommendation, Product
from data_mining.data_pipeline import DataPipeline

app = create_app()

PASS_COUNT = 0
FAIL_COUNT = 0


def check(label, condition, detail=""):
    global PASS_COUNT, FAIL_COUNT
    if condition:
        PASS_COUNT += 1
        print(f"    [PASS] {label}" + (f" — {detail}" if detail else ""))
    else:
        FAIL_COUNT += 1
        print(f"    [FAIL] {label}" + (f" — {detail}" if detail else ""))


with app.app_context():
    # Load actual transactions from SQL Server
    pipeline = DataPipeline()
    transactions = pipeline.get_transactions()
    pipeline.close()

    total = len(transactions)
    txn_sets = [set(t) for t in transactions]

    print("=" * 70)
    print("VALIDATION REPORT")
    print(f"Data source: SQL Server Cleaned_data")
    print(f"Total transactions: {total:,}")
    print("=" * 70)

    for algo in ['fpgrowth', 'apriori']:
        mr = MiningResult.query.filter_by(algorithm=algo).order_by(
            MiningResult.created_at.desc()
        ).first()

        if not mr:
            print(f"\n  [{algo.upper()}] No results found. Skipping.\n")
            continue

        results = json.loads(mr.results)
        params = json.loads(mr.parameters)
        rules = results.get('association_rules', [])
        itemsets = results.get('frequent_itemsets', [])

        print(f"\n{'=' * 70}")
        print(f"  ALGORITHM: {algo.upper()}")
        print(f"  Parameters: min_support={params.get('min_support')}, "
              f"min_confidence={params.get('min_confidence')}")
        print(f"  Results: {len(itemsets)} itemsets, {len(rules)} rules")
        print(f"{'=' * 70}")

        # ================================================================
        # TEST 1: Validate frequent itemsets
        # ================================================================
        print(f"\n  TEST 1: Frequent Itemsets (top 5 with 2+ items)")
        print(f"  {'—' * 50}")

        multi = [i for i in itemsets if len(i['items']) >= 2]
        for i, itemset in enumerate(multi[:5]):
            items = set(itemset['items'])
            manual_count = sum(1 for t in txn_sets if items <= t)
            manual_support = manual_count / total

            check(
                f"Itemset: {itemset['items']}",
                manual_count == itemset['freq'] and abs(manual_support - itemset['support']) < 0.0001,
                f"freq: algo={itemset['freq']} manual={manual_count}, "
                f"support: algo={itemset['support']:.6f} manual={manual_support:.6f}"
            )

        # ================================================================
        # TEST 2: Validate association rules
        # ================================================================
        print(f"\n  TEST 2: Association Rules (top 5 by lift)")
        print(f"  {'—' * 50}")

        for i, rule in enumerate(rules[:5]):
            ant = set(rule['antecedents'])
            con = set(rule['consequents'])
            full = ant | con

            ant_count = sum(1 for t in txn_sets if ant <= t)
            con_count = sum(1 for t in txn_sets if con <= t)
            full_count = sum(1 for t in txn_sets if full <= t)

            m_support = full_count / total
            m_confidence = full_count / ant_count if ant_count > 0 else 0
            m_lift = m_confidence / (con_count / total) if con_count > 0 else 0

            sup_ok = abs(m_support - rule['support']) < 0.0001
            conf_ok = abs(m_confidence - rule['confidence']) < 0.001
            lift_ok = abs(m_lift - rule['lift']) < 0.01

            check(
                f"Rule: {rule['antecedents']} -> {rule['consequents']}",
                sup_ok and conf_ok and lift_ok,
                f"support: {rule['support']:.6f}={m_support:.6f} | "
                f"confidence: {rule['confidence']:.4f}={m_confidence:.4f} | "
                f"lift: {rule['lift']:.4f}={m_lift:.4f}"
            )

        # ================================================================
        # TEST 3: Rules satisfy min thresholds
        # ================================================================
        print(f"\n  TEST 3: All rules meet min_support & min_confidence")
        print(f"  {'—' * 50}")

        min_sup = params.get('min_support', 0)
        min_conf = params.get('min_confidence', 0)

        bad_support = [r for r in rules if r['support'] < min_sup - 0.0001]
        bad_conf = [r for r in rules if r['confidence'] < min_conf - 0.001]

        check(
            f"All {len(rules)} rules have support >= {min_sup}",
            len(bad_support) == 0,
            f"{len(bad_support)} violations" if bad_support else "all valid"
        )
        check(
            f"All {len(rules)} rules have confidence >= {min_conf}",
            len(bad_conf) == 0,
            f"{len(bad_conf)} violations" if bad_conf else "all valid"
        )

        # ================================================================
        # TEST 4: No duplicate itemsets
        # ================================================================
        print(f"\n  TEST 4: No duplicate itemsets")
        print(f"  {'—' * 50}")

        seen_sets = set()
        dupes = 0
        for it in itemsets:
            key = tuple(sorted(it['items']))
            if key in seen_sets:
                dupes += 1
            seen_sets.add(key)

        check(
            f"Itemsets are unique",
            dupes == 0,
            f"{dupes} duplicates found" if dupes else f"all {len(itemsets)} unique"
        )

        # ================================================================
        # TEST 5: Lift > 1 means positive association
        # ================================================================
        print(f"\n  TEST 5: Lift interpretation")
        print(f"  {'—' * 50}")

        positive_rules = [r for r in rules if r['lift'] > 1]
        check(
            f"Rules with lift > 1 (positive association)",
            len(positive_rules) > 0,
            f"{len(positive_rules)}/{len(rules)} rules "
            f"({100*len(positive_rules)/len(rules):.0f}%)"
        )

    # ================================================================
    # TEST 6: Cross-algorithm consistency
    # ================================================================
    print(f"\n{'=' * 70}")
    print(f"  CROSS-ALGORITHM COMPARISON")
    print(f"{'=' * 70}")

    fp_mr = MiningResult.query.filter_by(algorithm='fpgrowth').order_by(
        MiningResult.created_at.desc()).first()
    ap_mr = MiningResult.query.filter_by(algorithm='apriori').order_by(
        MiningResult.created_at.desc()).first()

    if fp_mr and ap_mr:
        fp_rules = json.loads(fp_mr.results).get('association_rules', [])
        ap_rules = json.loads(ap_mr.results).get('association_rules', [])

        # Convert to comparable sets
        fp_rule_set = set()
        for r in fp_rules:
            key = (tuple(sorted(r['antecedents'])), tuple(sorted(r['consequents'])))
            fp_rule_set.add(key)

        ap_rule_set = set()
        for r in ap_rules:
            key = (tuple(sorted(r['antecedents'])), tuple(sorted(r['consequents'])))
            ap_rule_set.add(key)

        common = fp_rule_set & ap_rule_set
        fp_only = fp_rule_set - ap_rule_set
        ap_only = ap_rule_set - fp_rule_set

        print(f"\n  FP-Growth rules: {len(fp_rule_set)}")
        print(f"  Apriori rules:   {len(ap_rule_set)}")
        print(f"  Common rules:    {len(common)}")
        print(f"  FP-Growth only:  {len(fp_only)}")
        print(f"  Apriori only:    {len(ap_only)}")

        # With same parameters, they should find the same rules
        fp_params = json.loads(fp_mr.parameters)
        ap_params = json.loads(ap_mr.parameters)
        same_params = (fp_params.get('min_support') == ap_params.get('min_support') and
                       fp_params.get('min_confidence') == ap_params.get('min_confidence'))

        if same_params:
            overlap = len(common) / max(len(fp_rule_set), len(ap_rule_set)) * 100
            check(
                f"Same parameters: rules should match",
                overlap > 90,
                f"{overlap:.1f}% overlap"
            )
        else:
            print(f"\n  (Different parameters used — direct comparison skipped)")
            print(f"    FP-Growth: support={fp_params.get('min_support')}, "
                  f"confidence={fp_params.get('min_confidence')}")
            print(f"    Apriori:   support={ap_params.get('min_support')}, "
                  f"confidence={ap_params.get('min_confidence')}")

        # Spot-check: pick 3 common rules and verify both got same metrics
        print(f"\n  Spot-check: comparing metrics for common rules")
        print(f"  {'—' * 50}")

        fp_lookup = {}
        for r in fp_rules:
            key = (tuple(sorted(r['antecedents'])), tuple(sorted(r['consequents'])))
            fp_lookup[key] = r

        checked = 0
        for r in ap_rules:
            key = (tuple(sorted(r['antecedents'])), tuple(sorted(r['consequents'])))
            if key in fp_lookup and checked < 3:
                fp_r = fp_lookup[key]
                conf_match = abs(r['confidence'] - fp_r['confidence']) < 0.001
                lift_match = abs(r['lift'] - fp_r['lift']) < 0.01

                check(
                    f"{list(key[0])} -> {list(key[1])}",
                    conf_match and lift_match,
                    f"FP conf={fp_r['confidence']:.4f} lift={fp_r['lift']:.2f} | "
                    f"AP conf={r['confidence']:.4f} lift={r['lift']:.2f}"
                )
                checked += 1

    # ================================================================
    # TEST 7: Website recommendations are backed by rules
    # ================================================================
    print(f"\n{'=' * 70}")
    print(f"  WEBSITE RECOMMENDATIONS VERIFICATION")
    print(f"{'=' * 70}")

    test_names = [
        'WHITE HANGING HEART T-LIGHT HOLDER',
        'JUMBO BAG APPLES',
        'LUNCH BAG  BLACK SKULL.',
    ]

    for name in test_names:
        p = Product.query.filter_by(name=name).first()
        if not p:
            continue

        print(f"\n  Product: {name} (ID={p.id})")

        for algo in ['fpgrowth', 'apriori']:
            recs = Recommendation.query.filter_by(
                product_id=p.id, algorithm=algo
            ).order_by(Recommendation.lift.desc()).all()

            seen = set()
            unique = []
            for r in recs:
                if r.recommended_with_id not in seen:
                    seen.add(r.recommended_with_id)
                    unique.append(r)

            if not unique:
                continue

            print(f"    {algo.upper()} ({len(unique)} recs):")

            # Verify each rec exists in the mining results
            mr = MiningResult.query.filter_by(algorithm=algo).order_by(
                MiningResult.created_at.desc()).first()
            mining_rules = json.loads(mr.results).get('association_rules', []) if mr else []

            for r in unique[:3]:
                rp = db.session.get(Product, r.recommended_with_id)

                # Check if this product pair exists in any rule
                found_in_rules = False
                for rule in mining_rules:
                    all_items = set(rule['antecedents'] + rule['consequents'])
                    if name in all_items and rp.name in all_items:
                        found_in_rules = True
                        break

                # Also check itemsets
                mining_itemsets = json.loads(mr.results).get('frequent_itemsets', [])
                found_in_itemsets = False
                for it in mining_itemsets:
                    if name in it['items'] and rp.name in it['items']:
                        found_in_itemsets = True
                        break

                backed = found_in_rules or found_in_itemsets
                source = "rule" if found_in_rules else ("itemset" if found_in_itemsets else "NONE")

                check(
                    f"-> {rp.name}",
                    backed,
                    f"backed by {source}, lift={r.lift:.2f}, conf={r.confidence:.4f}"
                )

    # ================================================================
    # SUMMARY
    # ================================================================
    print(f"\n{'=' * 70}")
    total_tests = PASS_COUNT + FAIL_COUNT
    print(f"  RESULTS: {PASS_COUNT}/{total_tests} passed, {FAIL_COUNT} failed")
    if FAIL_COUNT == 0:
        print(f"  ALL TESTS PASSED")
    else:
        print(f"  FAILURES: {FAIL_COUNT}")
    print(f"{'=' * 70}")

    sys.exit(0 if FAIL_COUNT == 0 else 1)
