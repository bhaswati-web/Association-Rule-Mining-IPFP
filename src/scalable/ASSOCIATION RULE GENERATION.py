# ASSOCIATION RULE GENERATION

def generate_association_rules(
    partitioned_data,
    total_transactions
):

    print("PHASE 5 : ASSOCIATION RULE GENERATION")

    start_rules = time.time()

    # FINAL FP-GROWTH MODEL

    final_fp_growth = FPGrowth(
        itemsCol="items",
        minSupport=MIN_SUPPORT,
        minConfidence=MIN_CONFIDENCE
    )

    final_model = final_fp_growth.fit(
        partitioned_data
    )

    association_rules = (
        final_model.associationRules
        .cache()
    )

    # DISPLAY ASSOCIATION RULES

    sample_rules = (
        association_rules
        .limit(20)
        .collect()
    )

    print("\nAssociation Rules:\n")

    for row in sample_rules:

        antecedent = tuple(
            row["antecedent"]
        )

        consequent = tuple(
            row["consequent"]
        )

        confidence = row["confidence"]

        support = int(
            row["support"]
            * total_transactions
        )

        print(
            f"{antecedent} => "
            f"{consequent} | "
            f"Support: {support} | "
            f"Confidence: {confidence:.2f}"
        )

    # TOTAL RULES

    total_rules = (
        association_rules.count()
    )

    print(f"\nTotal Association Rules : "
          f"{total_rules}")

    end_rules = time.time()

    return (
        total_rules,
        start_rules,
        end_rules
    )
